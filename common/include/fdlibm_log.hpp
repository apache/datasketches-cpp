// fdlibm __ieee754_log, used by Java's StrictMath.log (and by Math.log on most JVMs).
// Derived from FDLIBM 5.3, Copyright (C) 1993 by Sun Microsystems, Inc.
// "Permission to use, copy, modify, and distribute this software is freely granted,
//  provided that this notice is preserved."
#ifndef FDLIBM_LOG_HPP
#define FDLIBM_LOG_HPP
#include <cstdint>
#include <cstring>
#include <limits>

namespace fdlibm {

inline int32_t hi_word(double x){ uint64_t u; std::memcpy(&u,&x,8); return (int32_t)(uint32_t)(u>>32); }
inline uint32_t lo_word(double x){ uint64_t u; std::memcpy(&u,&x,8); return (uint32_t)u; }
inline void set_hi_word(double& x, uint32_t hi){ uint64_t u; std::memcpy(&u,&x,8);
  u = (u & 0x00000000ffffffffULL) | ((uint64_t)hi<<32); std::memcpy(&x,&u,8); }

// Forces a value to be rounded to a double before it is used again. fdlibm needs strict
// IEEE-754 evaluation: a fused multiply-add anywhere in the polynomial below changes the
// result. Pragmas are overridden by an explicit -ffp-contract=fast, so use a volatile
// round-trip, which the standard requires the compiler to honour.
inline double rnd(double v) { volatile double t = v; return t; }

inline double log(double x) {
// fdlibm depends on strict IEEE-754 evaluation: a fused multiply-add would change the result
// of the polynomial evaluation below, so contraction must be off for this function.
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  static const double
    ln2_hi = 6.93147180369123816490e-01,  /* 3fe62e42 fee00000 */
    ln2_lo = 1.90821492927058770002e-10,  /* 3dea39ef 35793c76 */
    two54  = 1.80143985094819840000e+16,  /* 43500000 00000000 */
    Lg1 = 6.666666666666735130e-01,       /* 3FE55555 55555593 */
    Lg2 = 3.999999999940941908e-01,       /* 3FD99999 9997FA04 */
    Lg3 = 2.857142874366239149e-01,       /* 3FD24924 94229359 */
    Lg4 = 2.222219843214978396e-01,       /* 3FCC71C5 1D8E78AF */
    Lg5 = 1.818357216161805012e-01,       /* 3FC74664 96CB03DE */
    Lg6 = 1.531383769920937332e-01,       /* 3FC39A09 D078C69F */
    Lg7 = 1.479819860511658591e-01,       /* 3FC2F112 DF3E5244 */
    zero = 0.0;

  double hfsq,f,s,z,R,w,t1,t2,dk;
  int32_t k,hx,i,j;
  uint32_t lx;

  hx = hi_word(x);
  lx = lo_word(x);

  k = 0;
  if (hx < 0x00100000) {                       /* x < 2**-1022 */
    // fdlibm writes these as -two54/zero and (x-x)/zero, which also raise the divide-by-zero
    // and invalid flags. MSVC rejects a compile-time division by a zero constant (C2124), so
    // return the same values directly. The estimators never call log() with these inputs.
    if (((hx & 0x7fffffff) | lx) == 0) {                          /* log(+-0) = -inf */
      return -std::numeric_limits<double>::infinity();
    }
    if (hx < 0) {                                                 /* log(-#) = NaN */
      return std::numeric_limits<double>::quiet_NaN();
    }
    k -= 54; x *= two54;                       /* subnormal: scale up */
    hx = hi_word(x);
  }
  if (hx >= 0x7ff00000) { return x+x; }
  k += (hx>>20) - 1023;
  hx &= 0x000fffff;
  i = (hx + 0x95f64) & 0x100000;
  set_hi_word(x, (uint32_t)(hx | (i ^ 0x3ff00000)));   /* normalize x or x/2 */
  k += (i>>20);
  f = x - 1.0;
  if ((0x000fffff & (2+hx)) < 3) {             /* |f| < 2**-20 */
    if (f == zero) {
      if (k == 0) { return zero; }
      dk = (double)k; return rnd(dk*ln2_hi) + rnd(dk*ln2_lo);
    }
    R = rnd(rnd(f*f)*rnd(0.5 - rnd(0.33333333333333333*f)));
    if (k == 0) { return f-R; }
    dk = (double)k; return rnd(dk*ln2_hi) - (rnd(R - rnd(dk*ln2_lo)) - f);
  }
  s = f/(2.0+f);
  dk = (double)k;
  z = s*s;
  i = hx - 0x6147a;
  w = z*z;
  j = 0x6b851 - hx;
  t1 = rnd(w*rnd(Lg2 + rnd(w*rnd(Lg4 + rnd(w*Lg6)))));
  t2 = rnd(z*rnd(Lg1 + rnd(w*rnd(Lg3 + rnd(w*rnd(Lg5 + rnd(w*Lg7)))))));
  i |= j;
  R = t2 + t1;
  if (i > 0) {
    hfsq = rnd(0.5*f)*f;
    if (k == 0) { return f - rnd(hfsq - rnd(s*(hfsq+R))); }
    return rnd(dk*ln2_hi) - (rnd(hfsq - rnd(rnd(s*(hfsq+R)) + rnd(dk*ln2_lo))) - f);
  } else {
    if (k == 0) { return f - rnd(s*(f-R)); }
    return rnd(dk*ln2_hi) - (rnd(rnd(s*(f-R)) - rnd(dk*ln2_lo)) - f);
  }
}

} // namespace fdlibm
#endif
