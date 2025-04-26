
import enum

class SecurityTypes(str, enum.Enum):
    crypto = "crypto"
    forex = "forex"
    stocks = "stocks"

def security_type_from_str(security_type_str: str) -> SecurityTypes:
    if security_type_str not in valid_types():
        raise ValueError(f"Invalid security type: {security_type_str}")
    return SecurityTypes[security_type_str]

def valid_types() -> list[str]:
    return [ 
        SecurityTypes.crypto.value, 
        SecurityTypes.forex.value, 
        SecurityTypes.stocks.value 
    ]

if __name__ == "__main__":

    import unittest

    class Test(unittest.TestCase):
        def test_verify_sec_types_from_str(self):
            self.assertEqual(SecurityTypes.crypto, security_type_from_str("crypto"))

    unittest.main()