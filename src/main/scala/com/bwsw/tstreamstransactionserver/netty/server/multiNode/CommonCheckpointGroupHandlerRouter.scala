package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.GetConsumerStateHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.GetTransactionDataHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog.GetCommitLogOffsetsHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.consumer.PutConsumerCheckpointHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.{PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.{OpenTransactionHandler, PutTransactionHandler, PutTransactionsHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}
import io.netty.channel.ChannelHandlerContext

import scala.collection.Searching.{Found, _}
import scala.concurrent.ExecutionContext

class CommonCheckpointGroupHandlerRouter(server: TransactionServer,
                                         commonMaster: BookkeeperMaster,
                                         checkpointMaster: BookkeeperMaster,
                                         multiNodeCommitLogService: CommitLogService,
                                         packageTransmissionOpts: TransportOptions,
                                         authOptions: AuthenticationOptions,
                                         orderedExecutionPool: OrderedExecutionContextPool,
                                         notifier: OpenedTransactionNotifier,
                                         serverRoleOptions: CheckpointGroupRoleOptions,
                                         executionContext: ServerExecutionContextGrids,
                                         commitLogContext: ExecutionContext)
  extends RequestRouter{

  private implicit val authService =
    new AuthService(authOptions)

  private implicit val transportValidator =
    new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext

  private val (handlersIDs: Array[Byte], handlers: Array[RequestHandler]) = Array(

    handlerAuth(new GetCommitLogOffsetsHandler(
      multiNodeCommitLogService,
      serverReadContext
    )),

    handlerAuth(new PutStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new CheckStreamExistsHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new DelStreamHandler(
      server,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionIDHandler(
      server
    )),
    handlerAuth(new GetTransactionIDByTimestampHandler(
      server
    )),

    handlerAuthMetadata(new PutTransactionHandler(
      commonMaster,
      commitLogContext
    )),

    handlerAuthMetadata(new PutTransactionsHandler(
      checkpointMaster,
      commitLogContext
    )),

    handlerAuthData(new OpenTransactionHandler(
      server,
      commonMaster,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuth(new GetTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetLastCheckpointedTransactionHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new ScanTransactionsHandler(
      server,
      serverReadContext
    )),

    handlerAuthData(new PutProducerStateWithDataHandler(
      commonMaster,
      commitLogContext
    )),

    handlerAuthData(new PutSimpleTransactionAndDataHandler(
      server,
      commonMaster,
      notifier,
      authOptions,
      orderedExecutionPool
    )),

    handlerAuthData(new PutTransactionDataHandler(
      commonMaster,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionDataHandler(
      server,
      serverReadContext
    )),

    handlerAuthMetadata(new PutConsumerCheckpointHandler(
      commonMaster,
      commitLogContext
    )),
    handlerAuth(new GetConsumerStateHandler(
      server,
      serverReadContext
    )),

    handlerId(new AuthenticateHandler(
      authService
    )),
    handlerId(new IsValidHandler(
      authService
    )),

    handlerId(new GetMaxPackagesSizesHandler(
      packageTransmissionOpts
    )),

    handlerId(new GetZKCheckpointGroupServerPrefixHandler(
      serverRoleOptions
    ))
  ).sortBy(_._1).unzip



  override def route(message: RequestMessage,
                     ctx: ChannelHandlerContext): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.handle(message, ctx, None)
      case _ =>
      //        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}