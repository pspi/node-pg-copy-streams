"use strict";

const _ = require("lodash");
const assert = require("assert");
const async = require("async");
const concat = require("concat-stream");
const gonna = require("gonna");
const pg = require("pg");
const Writable = require("stream").Writable;

const code = require("../message-formats");
const copy = require("../").to;

const client = function () {
  const client = new pg.Client();
  client.connect();
  return client;
};

const testConstruction = function () {
  const txt = "COPY (SELECT * FROM generate_series(0, 10)) TO STDOUT";
  const stream = copy(txt, { highWaterMark: 10 });
  assert.equal(stream._readableState.highWaterMark, 10, "Client should have been set with a correct highWaterMark.");
};
testConstruction();

const testComparators = function () {
  const copy1 = copy();
  copy1.pipe(
    concat((buf) => {
      assert(copy1._gotCopyOutResponse, "should have received CopyOutResponse");
      assert(!copy1._remainder, "Message with no additional data (len=Int4Len+0) should not leave a remainder");
    })
  );
  copy1.end(new Buffer.from([code.CopyOutResponse, 0x00, 0x00, 0x00, 0x04]));
};
testComparators();

const testRange = function (top) {
  const fromClient = client();
  const txt = "COPY (SELECT * from generate_series(0, " + (top - 1) + ")) TO STDOUT";
  let res;

  const stream = fromClient.query(copy(txt));
  const done = gonna("finish piping out", 1000, () => {
    fromClient.end();
  });

  stream.pipe(
    concat((buf) => {
      res = buf.toString("utf8");
    })
  );

  stream.on("end", () => {
    const expected = _.range(0, top).join("\n") + "\n";
    assert.equal(res, expected);
    assert.equal(stream.rowCount, top, "should have rowCount " + top + " but got " + stream.rowCount);
    done();
  });
};
testRange(10000);

const testInternalPostgresError = function () {
  const cancelClient = client();
  const queryClient = client();

  const runStream = function (callback) {
    const txt = "COPY (SELECT pg_sleep(10)) TO STDOUT";
    const stream = queryClient.query(copy(txt));
    stream.on("data", (data) => {
      // Just throw away the data.
    });
    stream.on("error", callback);

    setTimeout(() => {
      const cancelQuery = "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query ~ 'pg_sleep' AND NOT query ~ 'pg_cancel_backend'";
      cancelClient.query(cancelQuery, () => {
        cancelClient.end();
      });
    }, 50);
  };

  runStream((err) => {
    assert.notEqual(err, null);
    const expectedMessage = "canceling statement due to user request";
    assert.notEqual(err.toString().indexOf(expectedMessage), -1, "Error message should mention reason for query failure.");
    queryClient.end();
  });
};
testInternalPostgresError();

const testNoticeResponse = function () {
  // we use a special trick to generate a warning
  // on the copy stream.
  const queryClient = client();
  let set = "";
  set += "SET SESSION client_min_messages = WARNING;";
  set += "SET SESSION standard_conforming_strings = off;";
  set += "SET SESSION escape_string_warning = on;";
  queryClient.query(set, (err, res) => {
    assert.equal(err, null, "testNoticeResponse - could not SET parameters");
    const runStream = function (callback) {
      const txt = "COPY (SELECT '\\\n') TO STDOUT";
      const stream = queryClient.query(copy(txt));
      stream.on("data", (data) => {});
      stream.on("error", callback);

      // make sure stream is pulled from
      stream.pipe(concat(callback.bind(null, null)));
    };

    runStream((err) => {
      assert.equal(err, null, err);
      queryClient.end();
    });
  });
};
testNoticeResponse();

const testClientReuse = function () {
  const c = client();
  const limit = 100000;
  const countMax = 10;
  let countA = countMax;
  let countB = 0;
  const runStream = function (num, callback) {
    const sql = "COPY (SELECT * FROM generate_series(0," + limit + ")) TO STDOUT";
    const stream = c.query(copy(sql));
    stream.on("error", callback);
    stream.pipe(
      concat((buf) => {
        const res = buf.toString("utf8");
        const exp = _.range(0, limit + 1).join("\n") + "\n";
        assert.equal(res, exp, "clientReuse: sent & received buffer should be equal");
        countB++;
        callback();
      })
    );
  };

  var rs = function (err) {
    assert.equal(err, null, err);
    countA--;
    if (countA) {
      runStream(countB, rs);
    } else {
      assert.equal(countB, countMax, "clientReuse: there should be countMax queries on the same client");
      c.end();
    }
  };

  runStream(countB, rs);
};
testClientReuse();

const testClientFlowingState = function () {
  const donePiping = gonna("finish piping out");
  const clientQueryable = gonna("client is still queryable after piping has finished");
  const c = client();

  // uncomment the code to see pausing and resuming of the connection stream

  //const orig_resume = c.connection.stream.resume;
  //const orig_pause = c.connection.stream.pause;
  //
  //c.connection.stream.resume = function () {
  //  console.log('resume', new Error().stack);
  //  orig_resume.apply(this, arguments)
  //}
  //
  //c.connection.stream.pause = function () {
  //  console.log('pause', new Error().stack);
  //  orig_pause.apply(this, arguments)
  //}

  const testConnection = function () {
    c.query("SELECT 1", () => {
      clientQueryable();
      c.end();
    });
  };

  const writable = new Writable({
    write(chunk, encoding, cb) {
      cb();
    },
  });
  writable.on("finish", () => {
    donePiping();
    setTimeout(testConnection, 100); // test if the connection didn't drop flowing state
  });

  const sql = "COPY (SELECT 1) TO STDOUT";
  const stream = c.query(copy(sql, { highWaterMark: 1 }));
  stream.pipe(writable);
};
testClientFlowingState();
