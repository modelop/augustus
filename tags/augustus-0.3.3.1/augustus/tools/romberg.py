def print_row(lst):
    print ' '.join('%11.8f' % x for x in lst)
 
def romberg(f, a, b, eps = 1E-8):
    """Approximate the definite integral of f from a to b by Romberg's method.
    eps is the desired accuracy."""
    R = [[0.5 * (b - a) * (f(a) + f(b))]]  # R[0][0]
    #print_row(R[0])
    n = 1
    while True:
        h = float(b-a)/2**n
        R.append((n+1)*[None])  # Add an empty row.
        R[n][0] = 0.5*R[n-1][0] + h*sum(f(a+(2*k-1)*h) for k in range(1, 2**(n-1)+1)) # for proper limits
        for m in range(1, n+1):
            R[n][m] = R[n][m-1] + (R[n][m-1] - R[n-1][m-1]) / (4**m - 1)
        #print_row(R[n])
        if abs(R[n][n-1] - R[n][n]) < eps:
            return R[n][n]
        n += 1

if __name__=='__main__':
  from math import *
  # In this example, the error function erf(1) is evaluated.
  x=romberg(lambda t: 2/sqrt(pi)*exp(-t*t), 0,1.0/sqrt(2.0))
  #print type(x),x*100
  x=100.0*x
  print '%5.2f'%x

