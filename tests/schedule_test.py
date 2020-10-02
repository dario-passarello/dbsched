import unittest
from dbsched.schedule import Schedule

cases = {
            'w0(x) w1(x) w2(y) r2(y) w3(x)': True,
            'w0(x) r2(x) r1(x) w2(x) w2(z)': True,
            'r1(x) r1(y) r2(z) r2(y) w2(y) w2(z) r1(z)': False,
            'r1(x) r2(x) w2(x) r1(x)': False,
            'r1(x) r2(x) w1(x) w2(x)': False,
            'w0(x) r1(x) w0(z) r1(z) r2(x) w0(y) r3(z) w3(z) w2(y) w1(x) w3(y)': True,
            'r1(x) w2(x) w1(x) w3(x)': True,
            'r5(x) r3(y) w3(y) r6(t) r5(t) w5(z) w4(x) r3(z) w1(y) r6(y) w6(t) w4(z) w1(t) w3(x) w1(x) r1(z) w2(t) w2(z)': False
        }

class ScheduleTest(unittest.TestCase):

    def test_VSR(self):

        for test, result in cases.items():
            self.assertEqual(result, Schedule(sched_str=test).VSR() is not None)
    
    def test_CSR(self):
        for test, result in cases.items():
            print(Schedule(sched_str=test).CSR())

if __name__ == '__main__':
    unittest.main()