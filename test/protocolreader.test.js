const code = require("../message-formats");
const { ProtocolReader } = require("../protocolreader");

function assertReaderEmitsEvents(inputByteArrays, expectedEvents) {
  const reader = new ProtocolReader();
  jest.spyOn(reader, "emit");

  for (const inputByteArray of inputByteArrays) {
    reader.read(Buffer.from(inputByteArray));
  }

  expect(reader.emit.mock.calls).toEqual(expectedEvents);
  jest.restoreAllMocks();
}

test("empty input gives empty result", () => {
  assertReaderEmitsEvents([], []);
});

test("message without content gives 'message'", () => {
  assertReaderEmitsEvents([[code.CopyOutResponse, 0x0, 0x0, 0x0, 0x4]], [["message", code.CopyOutResponse]]);
});

test("message with content gives 'message', 'content'", () => {
  assertReaderEmitsEvents(
    [[code.CopyData, 0x0, 0x0, 0x0, 0x5, 0x01]],
    [
      ["message", code.CopyData],
      ["content", Buffer.from([0x01])],
    ]
  );
});

test("message with content, and message without content gives 'message', 'content', 'message'", () => {
  assertReaderEmitsEvents(
    [[code.CopyData, 0x0, 0x0, 0x0, 0x5, 0x01, code.CopyDone, 0x0, 0x0, 0x0, 0x4]],
    [
      ["message", code.CopyData],
      ["content", Buffer.from([0x01])],
      ["message", code.CopyDone],
    ]
  );
});

test("message with content, and message with content gives 'message', 'content', 'message', 'content'", () => {
  assertReaderEmitsEvents(
    [[code.CopyData, 0x0, 0x0, 0x0, 0x5, 0x01, code.CopyData, 0x0, 0x0, 0x0, 0x5, 0x02]],
    [
      ["message", code.CopyData],
      ["content", Buffer.from([0x01])],
      ["message", code.CopyData],
      ["content", Buffer.from([0x02])],
    ]
  );
});

test("message with content where content is split into two chunks gives 'message', 'content', 'content'", () => {
  assertReaderEmitsEvents(
    [[code.CopyData, 0x0, 0x0, 0x0, 0x06, 0x01], [0x02]],
    [
      ["message", code.CopyData],
      ["content", Buffer.from([0x01])],
      ["content", Buffer.from([0x02])],
    ]
  );
});

test("message with content, where content is split into three chunks gives 'message', 'content', 'content', 'content'", () => {
  assertReaderEmitsEvents(
    [[code.CopyData, 0x0, 0x0, 0x0, 0x07, 0x01], [0x02], [0x03]],
    [
      ["message", code.CopyData],
      ["content", Buffer.from([0x01])],
      ["content", Buffer.from([0x02])],
      ["content", Buffer.from([0x03])],
    ]
  );
});

test("message that cuts at chunk boundary in all possible ways works", () => {
  const input = [code.CopyData, 0x0, 0x0, 0x0, 0x5, 0x01];
  const expectedEvents = [
    ["message", code.CopyData],
    ["content", Buffer.from([0x01])],
  ];

  for (let splitAt = 1; splitAt < input.length; splitAt++) {
    const inputPart1 = input.slice(0, splitAt);
    const inputPart2 = input.slice(splitAt);

    expect(inputPart1.length).toBeGreaterThan(0);
    expect(inputPart2.length).toBeGreaterThan(0);

    assertReaderEmitsEvents([inputPart1, inputPart2], expectedEvents);
  }
});
