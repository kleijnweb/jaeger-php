# jaeger-php

[![Tests](https://github.com/auxmoney/jaeger-php/actions/workflows/test.yaml/badge.svg)](https://github.com/auxmoney/jaeger-php/actions/workflows/test.yaml)
[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.1-8892BF.svg)](https://php.net/)
[![License](https://img.shields.io/github/license/auxmoney/jaeger-php.svg)](https://github.com/auxmoney/jaeger-php/blob/master/LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/auxmoney/jaeger-php/badge.svg?branch=master)](https://coveralls.io/github/auxmoney/jaeger-php?branch=master)

_ATTENTION: this is a fork and republication of [jukylin/jaeger-php](https://github.com/jukylin/jaeger-php)_

_We opted into forking and publishing the original library in order to maintain our set of 
[opentracing related symfony bundles](https://github.com/auxmoney?q=opentracingbundle). The original library seems to be unmaintained 
currently._

jaeger-php is a library implementing the [OpenTracing specification for PHP](https://github.com/opentracing/opentracing-php) to 
connect with the [Jaeger Distributed Tracing Platform](https://github.com/jaegertracing/jaeger). It can be used to instrument PHP
code to generate tracing data and send it to Jaeger.

## Installation

```
composer require auxmoney/jaeger-php
```

## Usage

First, you need to create a `Config` object, which serves as the factory to create your `Tracer`:

```php
// create a config instance
$config = \Jaeger\Config::getInstance();
// create a tracer
$tracer = $config->initTracer('example service name', '0.0.0.0:6831');
```

To make the distributed tracing work, you need to extract your `SpanContext` from somewhere, e.g. `$_SERVER`:
```php
$spanContext = $tracer->extract(\Opentracing\Formats\TEXT_MAP, $_SERVER);
```

You can then start tracing by using the common Opentracing interface:
```php
$tracer->startActiveSpan("example operation name", ['child_of' => $spanContext]);
```

To add metadata to your span, you need to retrieve it first (be sure to check the [semantic conventions](https://opentracing.io/specification/conventions/) first):
```php
$span = $tracer->getActiveSpan();
$span->addBaggageItem("user_id", "12345");
$span->setTag("http.url", "http://localhost");
$span->log(["message" => "responded successfully"]);
$span->finish();
```

Finally, at the end of your script, you should flush the original `Config`. This will flush all created `Tracer`s and all created `Span`s:
```php
$config->flush();
```

### optional configuration

```php
// optional: generate 128 bit trace ids (default: false)
$config->gen128bit();
// optional: disable tracing (default: false)
$config->setDisabled(true);
// optional: inject custom transport (default: TransportUdp)
$config->setTransport($transport);
// optional: inject custom reporter (default: RemoteReporter)
$config->setReporter($reporter);
// optional: inject custom sampler (default: ConstSampler)
$config->setSampler($sampler);
```

## Special thanks

Thank you, @jukylin, for creating this library!
