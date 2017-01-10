package netty.server

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import netty.{Descriptors, Message}
import transactionService.rpc.{TokenInvalidException, TransactionService}

import scala.concurrent.{Future => ScalaFuture}

class ServerHandler extends SimpleChannelInboundHandler[Message] {
  private implicit val context = netty.Context.serverPool.getContext

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    ServerHandler.invokeMethod(msg).map(message => ctx.writeAndFlush(message.toByteArray))
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    println("User is inactive")
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

private object ServerHandler {
  private implicit val context = netty.Context.serverPool.getContext
  val transactionServer = new TransactionServer()
  import Descriptors._
  def invokeMethod(message: Message): ScalaFuture[Message] = ScalaFuture{
    val (method, messageSeqId) = Descriptor.decodeMethodName(message)
    method match {
      case `putStreamMethod` =>
        ScalaFuture(Descriptors.PutStream.decodeRequest(message)) flatMap { args =>
          transactionServer.putStream(args.token, args.stream, args.partitions, args.description, args.ttl)
            .map(response => Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(Some(response)))(messageSeqId))
            .recover { case error =>
              println(error.getMessage)
              Descriptors.PutStream.encodeResponse(TransactionService.PutStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `doesStreamExistMethod` =>
        ScalaFuture(Descriptors.DoesStreamExist.decodeRequest(message)) flatMap { args =>
          transactionServer.doesStreamExist(args.token, args.stream)
            .map(response => Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.DoesStreamExist.encodeResponse(TransactionService.DoesStreamExist.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }


      case `getStreamMethod` =>
        ScalaFuture(Descriptors.GetStream.decodeRequest(message)) flatMap { args =>
          transactionServer.getStream(args.token, args.stream)
            .map(response => Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.GetStream.encodeResponse(TransactionService.GetStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `delStreamMethod` =>
        ScalaFuture(Descriptors.DelStream.decodeRequest(message)) flatMap { args =>
          transactionServer.delStream(args.token, args.stream)
            .map(response => Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.DelStream.encodeResponse(TransactionService.DelStream.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `putTransactionMethod` =>
        ScalaFuture(Descriptors.PutTransaction.decodeRequest(message)) flatMap { args =>
          transactionServer.putTransaction(args.token, args.transaction)
            .map(response => Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(Some(response)))(messageSeqId))
            .recover { case error =>
              println(error.getMessage)
              Descriptors.PutTransaction.encodeResponse(TransactionService.PutTransaction.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `putTranscationsMethod` =>
        ScalaFuture(Descriptors.PutTransactions.decodeRequest(message)) flatMap { args =>
          transactionServer.putTransactions(args.token, args.transactions)
            .map(response => Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(Some(response)))(messageSeqId))
            .recover { case error =>
              println(error.getMessage)
              Descriptors.PutTransactions.encodeResponse(TransactionService.PutTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `scanTransactionsMethod` =>
        ScalaFuture(Descriptors.ScanTransactions.decodeRequest(message)) flatMap { args =>
          transactionServer.scanTransactions(args.token, args.stream, args.partition, args.from, args.to)
            .map(response => Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.ScanTransactions.encodeResponse(TransactionService.ScanTransactions.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `putTransactionDataMethod` =>
        ScalaFuture(Descriptors.PutTransactionData.decodeRequest(message)) flatMap { args =>
          transactionServer.putTransactionData(args.token, args.stream, args.partition, args.transaction, args.data, args.from)
            .map(response => Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.PutTransactionData.encodeResponse(TransactionService.PutTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }


      case `getTransactionDataMethod` =>
        ScalaFuture(Descriptors.GetTransactionData.decodeRequest(message)) flatMap { args =>
          transactionServer.getTransactionData(args.token, args.stream, args.partition, args.transaction, args.from, args.to)
            .map(response => Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.GetTransactionData.encodeResponse(TransactionService.GetTransactionData.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }


      case `setConsumerStateMethod` =>
        ScalaFuture(Descriptors.SetConsumerState.decodeRequest(message)) flatMap { args =>
          transactionServer.setConsumerState(args.token, args.name, args.stream, args.partition, args.transaction)
            .map(response => Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.SetConsumerState.encodeResponse(TransactionService.SetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }


      case `getConsumerStateMethod` =>
        ScalaFuture(Descriptors.GetConsumerState.decodeRequest(message)) flatMap { args =>
          transactionServer.getConsumerState(args.token, args.name, args.stream, args.partition)
            .map(response => Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(Some(response)))(messageSeqId))
            .recover { case _ =>
              Descriptors.GetConsumerState.encodeResponse(TransactionService.GetConsumerState.Result(None, tokenInvalid = Some(transactionService.rpc.TokenInvalidException("error"))))(messageSeqId)
            }
        }

      case `authenticateMethod` =>
        ScalaFuture(Descriptors.Authenticate.decodeRequest(message)) map { args =>
          val response = transactionServer.authenticate(args.login, args.password)
            Descriptors.Authenticate.encodeResponse(TransactionService.Authenticate.Result(Some(response)))(messageSeqId)
        }


      case `isValidMethod` =>
        ScalaFuture(Descriptors.IsValid.decodeRequest(message)) map { args =>
          val response = transactionServer.isValid(args.token)
          Descriptors.IsValid.encodeResponse(TransactionService.IsValid.Result(Some(response)))(messageSeqId)
        }
    }
  }.flatMap(identity)
}
