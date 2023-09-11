<?php
/*
 * Copyright (c) 2019, The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

namespace tests\Transport;

use Jaeger\Jaeger;
use Jaeger\Reporter\RemoteReporter;
use Jaeger\Sampler\ConstSampler;
use Jaeger\ScopeManager;
use Jaeger\Sender\Sender;
use Jaeger\Thrift\Batch;
use Jaeger\Transport\TransportUdp;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TransportUdpTest extends TestCase
{
    /**
     * @var TransportUdp|null
     */
    public $tran;

    /**
     * @var Jaeger|null
     */
    public $tracer;


    /**
     * @var Sender|MockObject $senderMock
     */
    private $senderMock;

    public function setUp(): void
    {
        $this->senderMock = $this->createMock(Sender::class);
        $this->senderMock->method('emitBatch')->willReturn(true);

        $this->tran = new TransportUdp('localhost:6831', 0, $this->senderMock);

        $reporter = new RemoteReporter($this->tran);
        $sampler = new ConstSampler();
        $scopeManager = new ScopeManager();

        $this->tracer = new Jaeger('jaeger', $reporter, $sampler, $scopeManager);
    }

    public function testBuildAndCalcSizeOfProcessThrift(): void
    {
        $span = $this->tracer->startSpan('BuildAndCalcSizeOfProcessThrift');
        $span->finish();
        $this->tran->buildAndCalcSizeOfProcessThrift($this->tracer);
        static::assertEquals(95, $this->tran->procesSize);
    }

    public function testAppendBatching(): void
    {
        $spanCount = 1000;
        for ($i = 0; $i < $spanCount; $i++) {
            $span = $this->tracer->startSpan(
                __METHOD__ . "::$i",
                [
                    'tags' => [
                        'class' => __CLASS__,
                        'method' => __METHOD__,
                        'large' => str_repeat('xy', 100),
                    ],
                ]
            );
            $span->finish();
        }

        $batchNumber = 0;
        $flushedSpanCounts = [-1 => 0];
        $this->senderMock
            ->method('emitBatch')
            ->with($this->callback(function (Batch $batch) use (&$batchNumber, &$flushedSpanCounts) {
                $spanCount = count($batch->spans);
                $flushedSpanCounts[$batchNumber] = $spanCount;
                $flushedSpanCounts[-1] += $spanCount;
                $batchNumber++;
                return true;
            }));


        $this->tran->append($this->tracer);

        $this->assertEquals([
            -1 => 1000,
            0 => 152,
            1 => 152,
            2 => 152,
            3 => 152,
            4 => 152,
            5 => 152,
            6 => 88,
        ], $flushedSpanCounts);
    }
}
