"""mock_rest_server"""

from http.server import HTTPServer
from argparse import ArgumentParser
from threading import Thread
from time import sleep
from pathlib import Path
from ssl import SSLContext, PROTOCOL_TLSv1_2

from .database import JsonDatabase
from .server import JsonHttpRequestHandler


def main() -> None:
    """Main entry point to run the HTTP(s?) webserver from the CLI."""
    parser = ArgumentParser(description="HTTP Server")
    parser.add_argument("port", type=int, help="Listening port for HTTP Server")
    parser.add_argument("--ip", help="HTTP Server IP", default="0.0.0.0")
    parser.add_argument("--dbfile", type=Path, default="rest_service.db.json")
    parser.add_argument("--ssl-keyfile", type=Path)
    parser.add_argument("--ssl-certfile", type=Path)
    args = parser.parse_args()
    db_file: Path = args.dbfile.resolve()
    JsonDatabase.init(db_file)

    secured = False
    server = HTTPServer((args.ip, args.port), JsonHttpRequestHandler)
    if (
        args.ssl_keyfile
        and args.ssl_keyfile.is_file()
        and args.ssl_certfile
        and args.ssl_certfile.is_file()
    ):
        print("Enabling TLS")
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.load_cert_chain(
            certfile=str(args.ssl_certfile.resolve()),
            keyfile=str(args.ssl_keyfile.resolve()),
        )
        server.socket = ssl_context.wrap_socket(server.socket, server_side=True)
        secured = True
    print(
        f"{'HTTPS' if secured else 'HTTP'} JSON Server Running "
        f"from file {db_file}"
    )

    server_thread = Thread(target=server.serve_forever)
    server_thread.start()
    try:
        while server_thread.is_alive():
            sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting Down...")
        Thread(target=server.shutdown).start()
        JsonDatabase.instance().shutdown()


if __name__ == "__main__":
    main()
