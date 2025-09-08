package bike.rapido.dq.drift

object PSI { def psi(ref: Seq[Double], cur: Seq[Double]): Double = ref.zip(cur).map { case (p, q) => val pp = if (p <= 0) 1e-9 else p; val qq = if (q <= 0) 1e-9 else q; (pp - qq) * math.log(pp / qq) }.sum }

