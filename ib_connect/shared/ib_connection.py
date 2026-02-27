from ib_insync import IB

class IBConnection:
    """
    Connection management for Interactive Brokers using ib_insync.
    Provides a connected IB instance that can be used as a context manager.
    """

    @staticmethod
    def connect(host='127.0.0.1', port=7497, clientId=1, timeout=4.0, readonly=False, account=''):
        """
        Establish a connection to IB and return the connected IB instance (which is a context manager).
        
        Args:
            host (str): IB Gateway/TWS host. Default: '127.0.0.1'
            port (int): IB Gateway/TWS port. Default: 7497
            clientId (int): Client ID for the connection. Default: 1
            timeout (float): Connection timeout in seconds. Default: 4.0
            readonly (bool): Read-only connection. Default: False
            account (str): Account to use (optional). Default: ''
        
        Returns:
            IB: Connected IB instance (supports context manager: with ib: ...)
        
        Raises:
            ConnectionError: If connection fails.
        """
        ib = IB()
        try:
            ib.connect(host=host, port=port, clientId=clientId, timeout=timeout, readonly=readonly, account=account)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Interactive Brokers: {e}") from e
        
        return ib