#include "StreamerFactory.h"
#include "Streamer.h"

std::unique_ptr<FileWriter::IStreamer> FileWriter::KafkaStreamerFactory::create(
    const std::string &Broker, FileWriter::DemuxTopic &Demux,
    const FileWriter::StreamerOptions &Opts) {
  std::unique_ptr<FileWriter::Streamer> Streamer =
      std::make_unique<FileWriter::Streamer>(Broker, Demux.topic(), Opts);
  Streamer->setSources(Demux.sources());
  return Streamer;
}
