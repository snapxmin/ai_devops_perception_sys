"use strict";

const assert = require("node:assert/strict");
const { createIntentQueue } = require("../src/devops_perception/static/control.js");

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

async function tick() {
  await new Promise((resolve) => setImmediate(resolve));
}

async function main() {
  const gates = { old: deferred(), newest: deferred() };
  const executed = [];
  const applied = [];
  const pending = [];
  const queue = createIntentQueue(
    async (name) => {
      executed.push(name);
      await gates[name].promise;
      return name;
    },
    (name) => applied.push(name),
    (active, count) => pending.push([active, count]),
  );

  const old = queue.run("old");
  const newest = queue.run("newest");
  await tick();
  assert.deepEqual(executed, ["old"], "mutations must execute serially");

  gates.old.resolve();
  await tick();
  assert.deepEqual(executed, ["old", "newest"]);
  assert.deepEqual(applied, [], "a stale response must not update the dashboard");

  gates.newest.resolve();
  await Promise.all([old, newest]);
  assert.deepEqual(applied, ["newest"]);
  assert.equal(queue.latestIntent, 2);
  assert.deepEqual(pending[0], [true, 1]);
  assert.deepEqual(pending.at(-1), [false, 0]);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
