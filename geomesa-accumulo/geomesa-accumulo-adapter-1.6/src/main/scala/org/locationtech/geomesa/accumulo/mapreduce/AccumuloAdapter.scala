package org.locationtech.geomesa.accumulo.mapreduce

import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat

//import org.apache.accumulo.core.client.mapred.{AbstractInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.mapreduce.lib.impl.{InputConfigurator, ConfiguratorBase => ImplConfiguratorBase}
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase => UtilConfiguratorBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Level
import org.locationtech.geomesa.accumulo.mapreduce


object AccumuloAdapter {
//  def isConnectorInfoSet(conf: JobConf) = {
//    ImplConfiguratorBase.isConnectorInfoSet(classOf[AccumuloInputFormat], conf)
//  }
//
//  def setZooKeeperInstance(conf: JobConf, instance: String, zooKeepers: String) = {
//    AbstractInputFormat.setZooKeeperInstance(conf,
//      new ClientConfiguration().withInstance(instance).withZkHosts(zooKeepers))
//  }
//
  def setZooKeeperInstance(conf: Configuration, instance: String, zooKeepers: String) = {
    ImplConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf,
      new ClientConfiguration().withInstance(instance).withZkHosts(zooKeepers))

  }
//
  def setConnectorInfo(conf: Configuration, user: String, password: String) = {
//    org.apache.accumulo.core.client.mapred.AbstractInputFormat.setConnectorInfo(conf, user, new PasswordToken(password.getBytes()))
//    org.apache.accumulo.core.client.mapreduce.AbstractInputFormat.setConnectorInfo(conf, user, new PasswordToken(password.getBytes()))

    ImplConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, user,
      new PasswordToken(password.getBytes))
  }
//
//  def setConnectorInfo(conf: JobConf, user: String, password: String) = {
//    AbstractInputFormat.setConnectorInfo(conf, user, new PasswordToken(password.getBytes))
//  }
//
//  def setInputTableName(conf: JobConf, table: String) = {
//    InputFormatBase.setInputTableName(conf, table)
//  }
//
//  def setScanAuthorizations(conf: JobConf, auths: Authorizations) = {
//    AbstractInputFormat.setScanAuthorizations(conf, auths)
//  }
//
  def setScanAuthorizations(conf: Configuration, auths: Authorizations) = {
    InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, auths)
  }
//
//  def setLogLevel(conf: JobConf, level: Level) = {
//    AbstractInputFormat.setLogLevel(conf, level)
//  }


}
