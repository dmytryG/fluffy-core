# Fluffy core
Fluffy core Is a TS library for simplifying communication between microservices
in microservice architecture. It supports multiple providers such as
NATS, Kafka, RabbitMQ and RedisStreams and provides unified interface for fast integration
in a project.

[GitHub of a project](https://github.com/dmytryG/fluffy-core)
[NPM of a project](https://www.npmjs.com/package/fluffy-core?activeTab=readme)

# THIS PROJECT IS STILL UNDER DEVELOPMENT!
Please be careful in case of using it in production!

## Currently providers tested in request-response mode:
- NATS
- Kafka
- RabbitMQ
- RedisStreams

Please use it carefully and report any issues you find. I'll do my best to fix them.
But some things may not work as expected at current stage.

# How to use
It's simple, firstly create a provider object (you can use classes
NATSProvider, KafkaProvider, RabbitMQProvider, RedisStreamsProvider)
```typescript
const provider = new NATSProvider('localhost:4222')
```

Then you can pass the provider to FluffyCore constructor

```typescript
const fc = new FluffyCore(provider)
```

Register a route, when registering you are able to specify several fields:
 - controller - a controller is what actually makes some useful things (here
you should have your business logic)
 - topic - unique identifier of the route, you will make requests to this topic
 - middlewares - an array of controllers that will be executed before the controller
 - postware - an array of controllers that will be executed after the controller

 middleware and postware are executed in the order they are specified and are optional fields
 
you can register a route like this:
```typescript
fc.registerRoute({
    topic: 'test-topic',
        controller:
            async (msg: Message) => {
                console.log("Controller got message", msg)
                msg.resp = {
                    ok: true
                }
            },
        middlewares: [
            async (msg: Message) => {
                msg.safeMetadata = {
                    messageArrived: new Date(),
                }
            },
        ],
        postware: [
            async (msg: Message) => {
                msg.safeMetadata = {
                    ...msg.safeMetadata,
                    messageReadyToRespond: new Date(),
                }
            },
            async (msg: Message) => {
                msg.resp = {
                    ...msg.resp,
                    timeToProcess: msg.safeMetadata.messageReadyToRespond.getTime() - msg.safeMetadata.messageArrived.getTime()
                }
            }
        ],
})
```

You can store data such as authorization, userId context or something
like this in ```safeMetadata``` field. This data will be available in all middlewares
and postwares and will be cleared before sending response to the client.

Technically, you can use ```metadata``` field, but it's not recommended. This field
won't be cleaned and used for internal purposes.

```request``` field stores data of request, for example object
```json
{
  "hello": "world",
  "page": 1,
  "pets": [
    "dog",
    "cat" 
  ]
}
```

```id``` field is an unique uuid identifier of request, you shall not use it!!!

```resp``` is a field where you can save a response of your controller, this data will
be returned to your bussiness logic on the requester side.

You can set an error handler for cases when your business logic throws an error
```typescript
fc.setErrorProcessor(async ({ msg, e }: { msg: Message, e: any }) => {
        msg.resp = e
        msg.isError = true;
})
```

It's a recommended way to handle errors, you can customize it as you wish. 
It's basically just a controller.

After you configured your service, you call

```typescript
await fc.start()
```

to start listening for requests of make requests from this fc instance.

You can make a request using ```makeRequest``` method or ```makeRequestSimplify```.
```makeRequest```
method will return a response of your request as ```Message``` object and you
can do things with it as you wish.
```makeRequestSimplify``` will throw an error if request failed (if ```isError```
field is true) or return a response if it was successful. Only ```resp``` field
will be returned.

```typescript
await fc.makeRequestSimplify({
    topic: 'test-topic',
    data: { hello: 'world' }
})
```
OR
```typescript
await fc.makeRequest({
    topic: 'test-topic',
    data: {
        hello: 'world'
    }
})
```

You don't have to return anything from your controller, all modifications of 
response, context or metadata you should do inside ```msg``` object
that was provided to your controller.

You can use your own provider, if available options are not enough for you.
You just have to implement ```IProvider``` interface.

# Here are some performance tests
Perfomed in single thread, locally, on 100, 1000, 10000, 100000 and 1000000 requests averanged in request-reply mode.

| Technology    | RPS (Requests Per Second) |
|---------------|---------------------------|
| NATS          | 783                       |
| Kafka         | 330                       |
| Redis Streams | 649                       |
| RabbitMQ      | 664                       |


# In conclusion
The library is still under development, but it's already usable,
not stable, there are some tests that I have to perform, but it's
usable. And oh gosh how i sufferred several years ago when I
had to implement similar request-reply logic for Kafka, I hope
this library won't make you suffer too.
