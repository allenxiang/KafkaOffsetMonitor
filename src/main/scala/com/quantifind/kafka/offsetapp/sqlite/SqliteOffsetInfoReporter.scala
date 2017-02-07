package com.quantifind.kafka.offsetapp.sqlite

import java.util.concurrent.TimeUnit

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.{OWArgs, OffsetDB, OffsetInfoReporter}

class SQLiteOffsetInfoReporter(db: OffsetDB, args: OWArgs) extends OffsetInfoReporter {

  override def report(info: IndexedSeq[OffsetInfo]): Unit = {
    db.insertAll(info)
  }

  override def cleanupOldData() {
    db.emptyOld(System.currentTimeMillis - args.retain.toMillis)
    // Data older than 1 hour will be filtered.
    db.cleanupOld(System.currentTimeMillis - TimeUnit.HOURS.toMillis(1), args.refresh.toMillis)
  }
}
