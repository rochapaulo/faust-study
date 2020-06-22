import faust
import settings
from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer as AvroSerde


class AvroSchemaDecoder(faust.Schema):

    def __init__(self):
        super().__init__()
        self.serde = AvroSerde(CachedSchemaRegistryClient(settings.SCHEMA_REGISTRY_URL))

    def __avro_decode(self, encoded_message):
        return self.serde.decode_message(encoded_message)

    def loads_value(self, app, message, *, loads=None, serializer=None):
        return self.__avro_decode(message.value)

    def loads_key(self, app, message, *, loads=None, serializer=None):
        return self.__avro_decode(message.key)
