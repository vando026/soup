package conviva.soup

import conviva.soup.Size._

class SizeTest extends munit.FunSuite {

  test("nCV should be expected") {
    val t1 = nCV(cv0 = 0.05, ybar = 2, s2 = 20, N = 20000)   
    val t2 = nCV(cv0 = 0.05, ybar = 2, s2 = 20)   
    assertEquals(t1, 1819)
    assertEquals(t2, 2000)
  }

  test("nMoe should be expected") {
    val t1 = nMoe(e = .05, s2 = 2, N = 200, alpha = 0.05)   
    val t2 = nMoe(e = .05, s2 = 2, alpha = 0.05)   
    assertEquals(t1, 188)
    assertEquals(t2, 3074)
  }

  test("nPropMoe should be expected") {
    val t1 = nPropMoe(e = .01, pu = 0.04, alpha = 0.05)   
    val t2 = nPropMoe(e = .01, pu = 0.04, alpha = 0.05, N = 1000)   
    assertEquals(t1, 1476)
    assertEquals(t2, 597)
  }

  test("Neyman allocation should be expected") {
    val strata = Array("ESPN", "Byjus", "DMED")
    val n = 1e5
    val Sh  = Array(18.04, 13.20, 26.76)
    val Nh: Array[Double] = Array(902083, 31778, 119872)
    val t = nNeyman(strata, n, Nh, Sh)
    val t0 = t.nh
    val t1 = math.round(t0("ESPN"))
    val t2 = math.round(t0("Byjus"))
    val t3 = math.round(t0("DMED"))
    assertEquals(t1, 81773L)
    assertEquals(t2, 2108L)
    assertEquals(t3, 16119L)
  }


}
