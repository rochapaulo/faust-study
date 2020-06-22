import faust
import settings
from decoder import AvroSchemaDecoder

app = faust.App('my-app', broker=settings.KAFKA_BOOTSTRAP_SERVER)

t_in = app.topic('topic-in', schema=AvroSchemaDecoder())
t_out = app.topic('topic-out', schema=AvroSchemaDecoder())


def transform(value):
    return value


@app.agent(t_in, sink=[t_out])
async def process(stream):
    async for k, v in stream.items():
        if k['source'] == 'my-application':
            yield transform(v)


if __name__ == "__main__":
    print("Application Started")
    app.main()
