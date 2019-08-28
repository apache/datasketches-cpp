import unittest
from datasketches import hll_sketch, hll_union, tgt_hll_type

class HllTest(unittest.TestCase):
    def test_hll_sketch(self):
        k = 8
        n = 117
        hll = generate_sketch(n, k, tgt_hll_type.HLL_6)
        hll.update('string data')
        hll.update(3.14159) # double data

        self.assertLessEqual(hll.get_lower_bound(1), hll.get_estimate())
        self.assertLessEqual(hll.get_lower_bound(1), hll.get_composite_estimate())
        self.assertGreaterEqual(hll.get_upper_bound(1), hll.get_estimate())
        self.assertGreaterEqual(hll.get_upper_bound(1), hll.get_composite_estimate())

        self.assertEqual(hll.lg_config_k, k)
        self.assertEqual(hll.tgt_type, tgt_hll_type.HLL_6)

        bytes_compact = hll.serialize_compact()
        bytes_update = hll.serialize_updatable()
        self.assertEqual(len(bytes_compact), hll.get_compact_serialization_bytes())
        self.assertEqual(len(bytes_update), hll.get_updatable_serialization_bytes())

        self.assertFalse(hll.is_compact())
        self.assertFalse(hll.is_empty())

        self.assertIsNotNone(hll_sketch.get_rel_err(True, False, 12, 1))
        self.assertIsNotNone(hll_sketch.get_max_updatable_serialization_bytes(20, tgt_hll_type.HLL_6))

        hll.reset()
        self.assertTrue(hll.is_empty())

    def generate_sketch(self, n, k, type=tgt_hll_type.HLL_4, st_idx=0):
        sk = hll_sketch(k, type)
        for i in range(st_idx, st_idx + n):
            sk.update(i)
        return sk

    def test_hll_union(self):
        k = 7
        n = 53
        union = hll_union(k)

        sk = self.generate_sketch(n, k, tgt_hll_type.HLL_4, 0)
        union.update(sk)
        sk = self.generate_sketch(3 * n, k, tgt_hll_type.HLL_4, n)
        union.update(sk)
        union.update('string data')
        union.update(1.4142136)

        self.assertLessEqual(union.get_lower_bound(1), union.get_estimate())
        self.assertLessEqual(union.get_lower_bound(1), union.get_composite_estimate())
        self.assertGreaterEqual(union.get_upper_bound(1), union.get_estimate())
        self.assertGreaterEqual(union.get_upper_bound(1), union.get_composite_estimate())

        self.assertEqual(union.lg_config_k, k)
        self.assertFalse(union.is_compact())
        self.assertFalse(union.is_empty())
        
        


        
if __name__ == '__main__':
    unittest.main()
