/* What jq's Linux build takes from glibc and wasi-libc does not have. */
#include <math.h>

/* glibc's `gamma` is `lgamma`. */
double gamma(double x) { return lgamma(x); }
