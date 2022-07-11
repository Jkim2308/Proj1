import UserFunctions._

import scala.annotation.tailrec
import scala.io.StdIn.readLine

object Main {

  println("\nWelcome to Car Rental Query!\n")
  println("\nGet all the information you need for your car rental needs.\n")
  println("\nLet's get started!\n")

  @tailrec
   def mainPage(args: Array[String]): Unit = {
    println("[1] Login\n")
    println("[2] Create New Account\n")
    println("[3] Admin Login\n")
    println("[4] Exit\n")

    val choice = readLine("\nMake Your Selection: ")
    if (choice == "1") {
      login()
      //println("You have selected [1] to login")
    } else if (choice == "2") {
      createAccount()
      //println("You have selected [2] to create a new account")
    } else if (choice == "3") {
      adminLogin()
      //println("You have selected [3] to login as an admin")
    } else if (choice == "4") {
      println("Thank you for using Car Rental Query!")
      System.exit(0)
    } else {
      println("Invalid Selection. Please try again.\n")
      mainPage(args)
    }

  }
}
