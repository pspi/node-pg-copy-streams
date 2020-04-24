const _ = require("lodash");
const assert = require("assert");
const concat = require("concat-stream");
const gonna = require("gonna");
const pg = require("pg");
const Writable = require("stream").Writable;

const copyTo = require("..").to;

const getClientOld = function () {
  const client = new pg.Client();
  client.connect();
  return client;
};

const getClient = function (cb) {
  const client = new pg.Client();
  client.connect(() => cb(client));
};

function assertDatabaseCopyToResult(sql, assertFn) {
  const client = getClient((client) => {
    const chunks = [];

    const stream = client.query(copyTo(sql));

    const done = gonna("end client", 4000, () => {
      client.end();
    });

    stream.on("data", (chunk) => {
      chunks.push(chunk);
    });

    let doneCalled = false;
    stream.on("error", (err) => {
      if (!doneCalled) {
        doneCalled = true;
        done();
        assertFn(err);
      }
    });

    stream.on("end", () => {
      if (!doneCalled) {
        doneCalled = true;
        done();

        const result = Buffer.concat(chunks).toString();
        assertFn(null, chunks, result, stream);
      }
    });
  });
}

function executeSqlWithSeparateConnection(sql) {
  getClient((client) => {
    client.query(sql, () => {
      client.end();
    });
  });
}

test("passes options to parent stream", async () => {
  const sql = "COPY (SELECT 1) TO STDOUT";
  const stream = copyTo(sql, { highWaterMark: 10 });
  assert.equal(stream._readableState.highWaterMark, 10, "Client should have been set with a correct highWaterMark.");
});

test("internal postgres error ends copy to and emits error", (done) => {
  assertDatabaseCopyToResult("COPY (SELECT pg_sleep(10)) TO STDOUT", (err, chunks, result, stream) => {
    assert.notEqual(err, null);
    const expectedMessage = "canceling statement due to user request";
    assert.notEqual(err.toString().indexOf(expectedMessage), -1, "Error message should mention reason for query failure.");
    done();
  });

  setTimeout(() => {
    executeSqlWithSeparateConnection(
      "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query ~ 'pg_sleep' AND NOT query ~ 'pg_cancel_backend'"
    );
  }, 50);
});

test("interspersed NoticeResponse is ignored", (done) => {
  // we use a special trick to generate a warning on the copy stream.
  const queryClient = getClientOld();
  let set = "";
  set += "SET SESSION client_min_messages = WARNING;";
  set += "SET SESSION standard_conforming_strings = off;";
  set += "SET SESSION escape_string_warning = on;";
  queryClient.query(set, (err, res) => {
    assert.equal(err, null, "testNoticeResponse - could not SET parameters");
    const runStream = function (callback) {
      const txt = "COPY (SELECT '\\\n') TO STDOUT";
      const stream = queryClient.query(copyTo(txt));
      stream.on("error", callback);
      stream.pipe(concat(callback.bind(null, null)));
    };

    runStream((err) => {
      assert.equal(err, null, err);
      queryClient.end();
      done();
    });
  });
});

test("client can be reused for copy to", (done) => {
  const client = getClientOld();
  const generateRows = 100000;
  const runs = 10;
  let runsToStart = runs;
  let nthRun = 0;
  const runStream = function (num, callback) {
    const sql = "COPY (SELECT * FROM generate_series(0," + generateRows + ")) TO STDOUT";
    const stream = client.query(copyTo(sql));
    stream.on("error", callback);
    stream.pipe(
      concat((buf) => {
        const res = buf.toString("utf8");
        const exp = _.range(0, generateRows + 1).join("\n") + "\n";
        assert.equal(res, exp, "clientReuse: sent & received buffer should be equal");
        nthRun++;
        callback();
      })
    );
  };

  var rs = function (err) {
    assert.equal(err, null, err);
    runsToStart--;
    if (runsToStart) {
      runStream(nthRun, rs);
    } else {
      assert.equal(nthRun, runs, "clientReuse: there should be equal amount of queries on the same client");
      client.end();
      done();
    }
  };

  runStream(nthRun, rs);
});

test("client can be reused for query", (done) => {
  const donePiping = gonna("finish piping out");
  const clientQueryable = gonna("client is still queryable after piping has finished");
  const client = getClientOld();

  // uncomment the code to see pausing and resuming of the connection stream

  //const orig_resume = client.connection.stream.resume;
  //const orig_pause = client.connection.stream.pause;
  //
  //client.connection.stream.resume = function () {
  //  console.log('resume', new Error().stack);
  //  orig_resume.apply(this, arguments)
  //}
  //
  //client.connection.stream.pause = function () {
  //  console.log('pause', new Error().stack);
  //  orig_pause.apply(this, arguments)
  //}

  const testConnection = function () {
    client.query("SELECT 1", () => {
      clientQueryable();
      client.end();
      done();
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
  const stream = client.query(copyTo(sql, { highWaterMark: 1 }));
  stream.pipe(writable);
});

test("small rows are combined to single chunk", (done) => {
  const sql = "COPY (SELECT * FROM generate_series(1, 2)) TO STDOUT";
  assertDatabaseCopyToResult(sql, (err, chunks, result, stream) => {
    assert.equal(err, null);
    assert.equal(chunks.length, 1);
    assert.deepEqual(chunks[0], Buffer.from("1\n2\n"));
    done();
  });
});

test("large row spans multiple chunks", (done) => {
  const fieldSize = 64 * 1024;
  const sql = `COPY (SELECT repeat('-', ${fieldSize})) TO STDOUT`;
  assertDatabaseCopyToResult(sql, (err, chunks, result, stream) => {
    assert.equal(err, null);
    assert(chunks.length > 1);
    assert.equal(result, `${"-".repeat(fieldSize)}\n`);
    done();
  });
});

test("two small rows are combined to single chunk", (done) => {
  const sql = "COPY (SELECT * FROM generate_series(1, 2)) TO STDOUT";
  assertDatabaseCopyToResult(sql, (err, chunks, result, stream) => {
    assert.equal(err, null);
    assert.equal(chunks.length, 1);
    assert.deepEqual(chunks[0], Buffer.from("1\n2\n"));
    done();
  });
});

test("one large row spans multiple chunks", (done) => {
  const fieldSize = 64 * 1024;
  const sql = `COPY (SELECT repeat('-', ${fieldSize})) TO STDOUT`;
  assertDatabaseCopyToResult(sql, (err, chunks, result, stream) => {
    assert.equal(err, null);
    assert(chunks.length > 1);
    const expectedResult = `${"-".repeat(fieldSize)}\n`;
    assert.equal(result.length, expectedResult.length);
    assert.equal(result, expectedResult);
    done();
  });
});

test("provides row count", (done) => {
  const sql = "COPY (SELECT * FROM generate_series(1, 3)) TO STDOUT";
  assertDatabaseCopyToResult(sql, (err, chunks, result, stream) => {
    assert.equal(err, null);
    assert.equal(stream.rowCount, 3);
    done();
  });
});
