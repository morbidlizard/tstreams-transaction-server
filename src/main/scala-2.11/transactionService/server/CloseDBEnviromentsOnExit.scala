package transactionService.server

object CloseDBEnviromentsOnExit {
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      streamService.StreamServiceImpl.entityStore.close
      streamService.StreamServiceImpl.environment.close

      transactionMetaService.TransactionMetaServiceImpl.entityStore.close()
      transactionMetaService.TransactionMetaServiceImpl.environment.close()

      transactionService.server.сonsumerService.ConsumerServiceImpl.entityStore.close()
      transactionService.server.сonsumerService.ConsumerServiceImpl.environment.close()
    }
  })
}
