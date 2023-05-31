object test {
  def main(args: Array[String]): Unit = {
    import scala.io.StdIn
    val message = StdIn.readLine()
    val shift = StdIn.readInt()
    val secret = new Secret(message, shift)
    //加密
    secret.encrypt()
    //输出加密后的内容：密文
    println(secret)
    //解密
    secret.decrypt()
    //输出解密后的内容：明文
    println(secret)
  }
  class Secret(val message: String, val shift: Int) {
    def encrypt(): String = {
      message.map(c => if (c.isLetter) (c + shift).toChar else c)
    }

    def decrypt(): String = {
      message.map(c => if (c.isLetter) (c - shift).toChar else c)
    }

    override def toString(): String = message
  }
}
