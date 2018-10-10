#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._ 
object HelloWorld {
  def main(args: Array[String]) {
		if(args.length != 4){
			println("\nUsage: ./generate outputName startIndex endIndex numberRows.\n")
			System.exit(-1)
		}
		val pw = new PrintWriter(new File(args(0)))
		val start = args(1).toInt
		val end   = args(2).toInt	
		val rnd = new scala.util.Random
		var rowKey = 0
		val range = args(3).toInt 
		for (i <- 1 to range){
				rowKey = start + rnd.nextInt(end-start)
				pw.print(rowKey + "\n")
		}
		pw.close
  }
}

HelloWorld.main(args)
