import random
import time

class consts:
    @staticmethod
    def MILLIS_IN_SECONDS() -> int:
        return 1000

    @staticmethod
    def ONE_HOUR_IN_SECS() -> int:
        return 60 * 60
    
    @staticmethod
    def ONE_DAY_IN_SECS() -> int:
        return 24 * consts.ONE_HOUR_IN_SECS()
    
    @staticmethod
    def ONE_WEEK_IN_SECS() -> int:
        return 7 * consts.ONE_DAY_IN_SECS()
    
    @staticmethod
    def ONE_MONTH_IN_SECS() -> int:
        return 30 * consts.ONE_DAY_IN_SECS()

    @staticmethod
    def ONE_YEAR_IN_SECS() -> int:
        return 365 * consts.ONE_DAY_IN_SECS()

def unix_timestamp_secs_dec() -> float:
    return float(unix_timestamp_ms()) / float(consts.MILLIS_IN_SECONDS())

def unix_timestamp_secs() -> int:
    return int(time.time())

def unix_timestamp_ms() -> int:
    return int(time.time() * consts.MILLIS_IN_SECONDS())

def null_or_empty(string:str) -> bool:
    return string is None or len(string.strip()) == 0

def roll_dice_10percent() -> bool:
    return random.randint(1, 10) == 5

def roll_dice_5percent() -> bool:
    return random.randint(1, 20) == 10

def roll_dice_33percent() -> bool:
    return random.randint(1, 3) == 1

def rand_select(in_list:list[any]) -> any:
    if in_list is None or len(in_list) == 0:
        raise ValueError("in_list is required")
    return in_list[random.randint(0, len(in_list) - 1)]

if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):

        def test_null_or_empty(self):
            self.assertTrue(null_or_empty(None))
            self.assertTrue(null_or_empty(""))
            self.assertTrue(null_or_empty(" "))
            self.assertFalse(null_or_empty(" a "))

        def test_rand_select_one_mbr(self):
            test_arr = [1]
            result = rand_select(test_arr)
            self.assertEqual(1, result)

        def test_rand_select_no_mbrs(self):
            test_arr = []
            try:
                rand_select(test_arr)
            except ValueError:
                return
            self.assertTrue(False, "should not reach here")

        def test_rand_select_mbrs(self):
            test_arr = [1,0,2]
            result = rand_select(test_arr)
            self.assertIn(result, test_arr)

    unittest.main()