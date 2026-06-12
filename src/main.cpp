// kvmux — single-binary OpenAI-compatible LLM gateway.
//
// M2: the streaming proxy spine. A Boost.Beast/Asio C++20-coroutine server
// fronts a configured backend, re-streaming SSE token-by-token with per-frame
// flush, injecting stream_options.include_usage upstream and forwarding the
// trailing usage chunk and [DONE]. SIGTERM triggers a graceful drain: new
// requests get 503 while in-flight streams finish.

#include "config.hpp"
#include "server/gateway.hpp"
#include "server/listener.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>

namespace asio = boost::asio;
using tcp = boost::asio::ip::tcp;
using namespace std::chrono_literals;

namespace {
constexpr const char* kVersion = "0.1.0";

void print_usage() {
    std::cout << "kvmux " << kVersion << "\n"
              << "usage: kvmux --config <path.toml>\n"
              << "       kvmux --version\n";
}
} // namespace

int main(int argc, char** argv) {
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--config" || arg == "-c") && i + 1 < argc) {
            config_path = argv[++i];
        } else if (arg == "--version") {
            std::cout << "kvmux " << kVersion << "\n";
            return 0;
        } else if (arg == "--help" || arg == "-h") {
            print_usage();
            return 0;
        }
    }

    if (config_path.empty()) {
        std::cerr << "error: --config <path.toml> is required\n";
        print_usage();
        return 2;
    }

    kvmux::config::Config cfg;
    try {
        cfg = kvmux::config::load_file(config_path);
    } catch (const kvmux::config::ConfigError& e) {
        std::cerr << "config error: " << e.what() << std::endl;
        return 2;
    }

    std::cout << "kvmux " << kVersion << " — loaded " << config_path << " (" << cfg.backends.size()
              << " backend(s), policy=" << kvmux::config::to_string(cfg.routing.policy) << ")"
              << std::endl;

    try {
        asio::io_context ioc(1);
        kvmux::server::Gateway gateway(ioc, cfg);
        gateway.start_health_monitors();

        auto address = asio::ip::make_address(cfg.server.listen);
        tcp::endpoint endpoint(address, static_cast<unsigned short>(cfg.server.port));

        asio::co_spawn(ioc, kvmux::server::run_listener(endpoint, gateway),
                       [](std::exception_ptr e) {
                           if (e) {
                               try {
                                   std::rethrow_exception(e);
                               } catch (const std::exception& ex) {
                                   std::cerr << "listener error: " << ex.what() << std::endl;
                               }
                           }
                       });

        // Graceful drain on SIGINT/SIGTERM: flip the gateway to draining (new
        // requests -> 503), give in-flight streams a bounded window to finish,
        // then stop the io_context.
        asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](const boost::system::error_code&, int sig) {
            std::cout << "\nreceived signal " << sig << "; draining..." << std::endl;
            gateway.begin_drain();
            gateway.stop_health_monitors();

            // Poll for in-flight to reach zero, up to a 30s cap, then stop. The
            // poller keeps itself alive via shared_ptr so it survives past this
            // handler's return.
            struct Drainer : std::enable_shared_from_this<Drainer> {
                asio::io_context& ioc;
                kvmux::server::Gateway& gw;
                asio::steady_timer timer;
                std::chrono::steady_clock::time_point deadline;
                Drainer(asio::io_context& i, kvmux::server::Gateway& g)
                    : ioc(i), gw(g), timer(i), deadline(std::chrono::steady_clock::now() + 30s) {}
                void poll() {
                    if (gw.in_flight() == 0 || std::chrono::steady_clock::now() >= deadline) {
                        std::cout << "drain complete (in_flight=" << gw.in_flight() << ")"
                                  << std::endl;
                        ioc.stop();
                        return;
                    }
                    timer.expires_after(100ms);
                    auto self = shared_from_this();
                    timer.async_wait([self](const boost::system::error_code&) { self->poll(); });
                }
            };
            std::make_shared<Drainer>(ioc, gateway)->poll();
        });

        ioc.run();
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
