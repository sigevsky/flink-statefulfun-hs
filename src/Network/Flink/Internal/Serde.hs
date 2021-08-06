{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Network.Flink.Internal.Serde (
  Serde (..),
  ProtoSerde (..),
  JsonSerde (..),
  unwrapA,
  unwrapA',
  Mappers (..),
  SerdeError (..),
  Json,
) where

import Control.Monad.Except.CoHas
import Data.Aeson
import Data.ByteString
import qualified Data.ByteString.Lazy as BSL
import Data.Data
import Data.ProtoLens
import Data.Text
import GHC.Generics (Generic)
import Lens.Family2
import qualified Proto.RequestReply as PR
import qualified Proto.RequestReply_Fields as PR

class Serde a where
  -- | Type name
  tpName :: Proxy a -> Text

  -- | decodes types from strict 'ByteString's
  deserializeBytes :: ByteString -> Either String a

  -- | encodes types to strict 'ByteString's
  serializeBytes :: a -> ByteString

newtype ProtoSerde a = ProtoSerde {getProto :: a}
  deriving (Functor)

instance Message a => Serde (ProtoSerde a) where
  tpName px = "type.googleapis.com/" <> messageName (unliftP px)
   where
    unliftP :: Proxy (f a) -> Proxy a
    unliftP Proxy = Proxy
  deserializeBytes a = ProtoSerde <$> decodeMessage a
  serializeBytes (ProtoSerde a) = encodeMessage a

type Json a = (FromJSON a, ToJSON a)

newtype JsonSerde a = JsonSerde {getJson :: a}
  deriving (Functor)

instance Json a => Serde (JsonSerde a) where
  tpName _ = "json/json" -- TODO: add adt name
  deserializeBytes a = JsonSerde <$> eitherDecode (BSL.fromStrict a)
  serializeBytes (JsonSerde a) = BSL.toStrict $ encode a

instance Serde () where
  tpName _ = "ghc/Unit"
  deserializeBytes _ = pure ()
  serializeBytes _ = ""

instance Serde ByteString where
  tpName _ = "ghc/Data.ByteString"
  deserializeBytes = pure
  serializeBytes = id

instance Serde BSL.ByteString where
  tpName _ = "ghc/Data.ByteString.Lazy"
  deserializeBytes = pure . BSL.fromStrict
  serializeBytes = BSL.toStrict

data SerdeError = InvalidTypePassedError [Text] Text | MessageDecodeError String
  deriving (Show, Eq, Generic)

data Mappers (as :: [*]) a where
  MZ :: Mappers '[] a
  MS :: Serde d => (d -> a) -> Mappers as a -> Mappers (d ': as) a

availableMappers :: forall as a. Mappers as a -> [Text]
availableMappers MZ = []
availableMappers (MS (_ :: d -> a) next) = tpName @d Proxy : availableMappers next

unwrapA :: forall a as e m. (Serde a, MonadError e m, CoHas SerdeError e) => Mappers as a -> PR.TypedValue -> m (Maybe a)
unwrapA mps tv = if not (tv ^. PR.hasValue) then pure Nothing else Just <$> go complete
 where
  complete = MS id mps
  go :: forall bs b. Mappers bs b -> m b
  go MZ = throwError $ InvalidTypePassedError (availableMappers mps) (tv ^. PR.typename)
  go (MS (_ :: d -> b) nextMp) | tpName @d Proxy /= tv ^. PR.typename = go nextMp
  go (MS mp nextMp) = case deserializeBytes (tv ^. PR.value) of
    Right a -> pure $ mp a
    Left _ -> go nextMp

unwrapA' :: (Serde a, MonadError e m, CoHas SerdeError e) => PR.TypedValue -> m (Maybe a)
unwrapA' = unwrapA MZ