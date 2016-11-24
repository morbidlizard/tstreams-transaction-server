package resource

import java.io.FileInputStream
import java.util.Properties

class ConfigServer(pathToConfig: String) {
  private lazy val properties = {
    val file = new Properties()
    val in = new FileInputStream(pathToConfig)
    file.load(in)
    in.close()
    file
  }


}
