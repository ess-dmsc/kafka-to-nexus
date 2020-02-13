// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "PartitionStream.h"

PartitionStream::PartitionStream(std::unique_ptr<KafkaW::Consumer> Consumer, SrcToDst Map, Metrics::Registrar RegisterMetric,
                                 time_point Stop) : ConsumerPtr(std::move(Consumer)), DataMap(Map), Registrar(std::move(RegisterMetric)), StopTime(Stop) {
}

void PartitionStream::setStopTime(time_point Stop) {
  Executor.SendWork([=](){
    StopTime = Stop;
  });
}

bool PartitionStream::hasFinished() {
  return HasFinished.load();
}

