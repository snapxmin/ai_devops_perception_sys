(function exposeIntentQueue(root, factory) {
  "use strict";

  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  } else {
    root.PerceptionControl = api;
  }
}(typeof globalThis === "object" ? globalThis : this, function intentQueueFactory() {
  "use strict";

  function createIntentQueue(execute, apply = () => {}, onPendingChange = () => {}) {
    let latestIntent = 0;
    let pendingCount = 0;
    let tail = Promise.resolve();

    function run(value) {
      const intent = ++latestIntent;
      pendingCount += 1;
      onPendingChange(true, pendingCount);

      const task = tail
        .catch(() => undefined)
        .then(() => execute(value, intent))
        .then((result) => {
          if (intent === latestIntent) return apply(result, intent);
          return result;
        });
      tail = task;

      return task.finally(() => {
        pendingCount -= 1;
        onPendingChange(pendingCount > 0, pendingCount);
      });
    }

    return {
      run,
      get latestIntent() {
        return latestIntent;
      },
    };
  }

  return { createIntentQueue };
}));
