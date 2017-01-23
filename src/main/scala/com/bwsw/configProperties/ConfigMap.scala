package com.bwsw.configProperties

class ConfigMap(map: scala.collection.immutable.Map[String,String]) extends Config {
  override val properties: Map[String, String] = map
}
