{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Flink.Internal.TypedStateful (
  FuncSpec (..),
  TypedStateful (..),
  Ref,
  Ref',
  mkFunc,
  mkFunc',
  mkServer,
) where

import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Data.Kind (Constraint)
import GHC.TypeLits (ErrorMessage (Text), TypeError)
import Network.Flink.Internal.Stateful

import qualified Data.Map as M
import Network.Flink.Internal.Serde

import Deriving.Aeson
import Servant (Server)

newtype Ref (as :: [*]) = Ref Address
  deriving (Show, Eq, Generic)
  deriving (ToJSON, FromJSON) via CustomJSON '[FieldLabelModifier '[CamelToSnake]] (Ref as)

type Ref' a = Ref '[a]

type family HasTy (l :: [*]) b :: Constraint where
  HasTy '[] _ = TypeError ( 'Text "BOOM!")
  HasTy (a ': as) a = ()
  HasTy (a ': as) b = HasTy as b

class StatefulFunc s f => TypedStateful as s f | f -> as, f -> s where
  self :: HasTy as a => f (Ref' a)
  tell :: (Serde c, HasTy cs c) => Ref cs -> c -> f ()
  replying :: (Serde c, HasTy cs c, HasTy as a) => Ref cs -> (Ref' a -> c) -> f ()

newtype FuncM (as :: [*]) s a = FuncM {runFuncM :: Function s a}
  deriving (Monad, Applicative, Functor, MonadError FlinkError, MonadIO, MonadReader Env)
  deriving (StatefulFunc s) via (Function s)

instance TypedStateful as s (FuncM as s) where
  self = asks (Ref . eself)
  tell (Ref addr) = sendMsg addr
  replying (Ref addr) f = self >>= sendMsg addr . f

data FuncSpec s = FuncSpec
  { s0 :: s
  , ftype :: FuncType
  , expr :: Expiration
  }

mkFunc' ::
  (Serde a, Serde s) =>
  -- | Mappers transforming different inputs to @a
  Mappers as a ->
  FuncSpec s ->
  -- | Dsl for a function
  (a -> FuncM (a ': as) s ()) ->
  -- | Reference to a function along
  -- with a runtime to process request emitted via constructed reference
  (FuncId -> Ref (a ': as), FuncExec)
mkFunc' mps (FuncSpec s0' ft expr') f = (Ref . Address ft, body')
 where
  body' = flinkWrapper' mps s0' expr' (runFuncM . f)

mkFunc ::
  (Serde a, Serde s, Monad f) =>
  -- | Mappers transforming different inputs to @a
  Mappers as a ->
  FuncSpec s ->
  -- | Dsl for a function
  (a -> FuncM (a ': as) s ()) ->
  StateT FunctionTable f (FuncId -> Ref (a ': as))
mkFunc mpps fs f = do
  modify (M.insert (ftype fs) body)
  return ref
 where
  (ref, body) = mkFunc' mpps fs f

mkServer :: Monad f => StateT FunctionTable f () -> f (Server FlinkApi)
mkServer = fmap flinkServer . flip execStateT M.empty