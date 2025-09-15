"""
A Requests session.

Provides cookie persistence, connection-pooling, and configuration.

Basic Usage:

  >>> import requests
  >>> s = requests.Session()
  >>> s.get('https://httpbin.org/get')
  <Response [200]>
Or as a context manager:

  >>> with requests.Session() as s:
  ...     s.get('https://httpbin.org/get')
  <Response [200]>
"""

def main() -> None:
    ...


if __name__ == "__main__":
    main()
