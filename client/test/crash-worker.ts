import { Worker } from "../src/index";
new Worker("crash", async () => {
  console.log("CLAIMED");
  await new Promise(() => {});
}, { address: process.env.ANVILMQ_ADDR });
