#include "Registrar.h"
#include "Metric.h"
#include <algorithm>

namespace Metrics {

void Registrar::registerMetric(Metric &NewMetric,
                               std::vector<LogTo> const &SinkTypes) const {
  if (NewMetric.getName().empty()) {
    throw std::runtime_error("Metrics cannot be registered with an empty name");
  }
  for (auto const &reporter : ReporterList) {
    if (std::find(SinkTypes.begin(), SinkTypes.end(),
                  reporter->getSinkType()) != SinkTypes.end()) {
      std::string NewName = prependPrefix(NewMetric.getName());

      if (!reporter->addMetric(NewMetric, NewName)) {
        throw std::runtime_error(
            "Metric with same full name is already registered");
      }
      NewMetric.setDeregistrationDetails(NewName, reporter);
    }
  }
}

void Registrar::initServer() {
  sockaddr_in address{};
  int opt = 1, addrlen = sizeof(address);

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
    throw std::runtime_error("Failed to init socket!");

  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY; //	restrict to 10.100.x.y?
  address.sin_port = htons(9999);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    throw std::runtime_error("Failed to bind to port");

  if (listen(server_fd, 3) < 0)
    throw std::runtime_error("Failed to listen");

  while (true) { //	threaded connections?
    client_fd =
        accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);

    std::string response_body;

    // Build the metric key using ApplicationStatusInfo
    std::string metric_key = Prefix + ".worker_state";

    // Find worker_state metric directly by key
    for (auto const &reporter : ReporterList) {
      auto metrics = reporter->getMetrics();
      auto it = metrics.find(metric_key);
      if (it != metrics.end()) {
        response_body = "{\"" + metric_key + "\": " + it->second.Value() + "}";
        break;
      }
    }

    // Build a valid JSON array string
    std::string response;
    if (!response_body.empty()) {
      response = "[" + response_body + "]";
    } else {
      response = "[]";
    }

    send(client_fd, response.c_str(), response.size(), 0);
    shutdown(client_fd, SHUT_WR);
    close(client_fd);
  }
}

void Registrar::killServer() { //	where/when/should we ever call this?
  close(server_fd);
}

std::unique_ptr<IRegistrar>
Registrar::getNewRegistrar(std::string const &MetricsPrefix) const {
  return std::make_unique<Registrar>(prependPrefix(MetricsPrefix),
                                     ReporterList);
}

std::string Registrar::prependPrefix(std::string const &Name) const {
  if (Prefix.empty()) {
    return Name;
  }
  if (Name == "")
    return Prefix;
  return {Prefix + "." + Name};
}
} // namespace Metrics
