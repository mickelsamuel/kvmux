#pragma once

// TCP listener + per-connection session loop. Accepts connections and runs a
// keep-alive request loop per connection, dispatching each request to the
// Gateway. Coroutine-based on Boost.Asio.

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace kvmux::server {

class Gateway;

// Accept loop: listens on `endpoint`, spawns a session coroutine per connection.
// Runs until the acceptor is closed (graceful shutdown).
boost::asio::awaitable<void> run_listener(boost::asio::ip::tcp::endpoint endpoint,
                                          Gateway& gateway);

} // namespace kvmux::server
