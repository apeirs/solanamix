// An unmodified @solana/web3.js 1.98 client pointed at `solanamix serve`. It
// knows nothing about Nym: only the RPC URL differs. web3.js derives the
// PubSub URL itself (port + 1, same path): ws://127.0.0.1:8900/devnet.
//
//   solanamix serve &
//   node --require ./no-bypass-hook.cjs web3-client.mjs [http://127.0.0.1:8899]
//
// With SOLANAMIX_TEST_KEYFILE=key.json (a fresh, unfunded devnet key in
// solana-keygen format) it also signs a 1-lamport devnet transfer locally,
// writes it base64 to SOLANAMIX_TEST_TXFILE (for `solanamix send-raw`) and sends it.
// The key is never printed. Devnet has no account for it, so the node must
// reject it.
import { readFileSync, writeFileSync } from 'node:fs';
import { Connection, Keypair, PublicKey, SystemProgram, Transaction, LAMPORTS_PER_SOL } from '@solana/web3.js';

const base = process.argv[2] || 'http://127.0.0.1:8899';
const USDC = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
const holder = new PublicKey('9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM');

async function step(name, fn) {
  const t0 = performance.now();
  try {
    console.log(`${name}: ${await fn()}  (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
  } catch (e) {
    const msg = (e?.message ?? String(e)).split('\n').map((l) => l.trim()).filter(Boolean).join(' | ');
    console.log(`${name}: ERROR ${msg.slice(0, 240)}  (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
  }
}

const mainnet = new Connection(base, 'confirmed');
const devnet = new Connection(base + '/devnet', 'confirmed');
const testnet = new Connection(base + '/testnet', 'confirmed');
console.log(`@solana/web3.js, RPC ${base} (mainnet), ${base}/devnet, ${base}/testnet; PubSub derived by web3.js`);

await step('mainnet getGenesisHash', () => mainnet.getGenesisHash());
await step('mainnet getSlot', async () => `${await mainnet.getSlot()}`);
await step('mainnet getBalance', async () => `${(await mainnet.getBalance(holder)) / LAMPORTS_PER_SOL} SOL`);
await step('mainnet getParsedAccountInfo(USDC mint)', async () => {
  const info = (await mainnet.getParsedAccountInfo(USDC)).value.data.parsed.info;
  return `supply ${Number(info.supply) / 10 ** info.decimals} USDC, ${info.decimals} decimals`;
});
await step('mainnet getRecentPrioritizationFees', async () => `${(await mainnet.getRecentPrioritizationFees()).length} slots`);
await step('devnet getGenesisHash', () => devnet.getGenesisHash());
await step('devnet getVersion', async () => (await devnet.getVersion())['solana-core']);
await step('devnet getEpochInfo', async () => {
  const e = await devnet.getEpochInfo();
  return `epoch ${e.epoch}, slot ${e.absoluteSlot}, block height ${e.blockHeight}`;
});
await step('testnet getGenesisHash', () => testnet.getGenesisHash());
await step('devnet onSlotChange (PubSub websocket), 3 notifications', () => new Promise((resolve, reject) => {
  const slots = [];
  const timer = setTimeout(() => reject(new Error('no slot notifications in 60 s')), 60000);
  const id = devnet.onSlotChange((s) => {
    slots.push(s.slot);
    if (slots.length === 3) {
      clearTimeout(timer);
      devnet.removeSlotChangeListener(id).then(() => resolve(`slots ${slots.join(', ')}`));
    }
  });
}));
await step('mainnet onAccountChange subscribe + unsubscribe (PubSub websocket)', async () => {
  const id = mainnet.onAccountChange(holder, () => {});
  await new Promise((r) => setTimeout(r, 8000));
  await mainnet.removeAccountChangeListener(id);
  return 'subscribed and removed';
});

const keyfile = process.env.SOLANAMIX_TEST_KEYFILE;
if (keyfile) {
  const kp = Keypair.fromSecretKey(Uint8Array.from(JSON.parse(readFileSync(keyfile))));
  console.log(`fresh devnet key ${kp.publicKey.toBase58()} (created for this test, never funded)`);
  await step('devnet getBalance(fresh key)', async () => `${await devnet.getBalance(kp.publicKey)} lamports`);
  let tx;
  await step('devnet build + sign 1-lamport transfer locally', async () => {
    const { blockhash } = await devnet.getLatestBlockhash();
    tx = new Transaction({ feePayer: kp.publicKey, recentBlockhash: blockhash })
      .add(SystemProgram.transfer({ fromPubkey: kp.publicKey, toPubkey: Keypair.generate().publicKey, lamports: 1 }));
    tx.sign(kp);
    const raw = tx.serialize();
    if (process.env.SOLANAMIX_TEST_TXFILE) writeFileSync(process.env.SOLANAMIX_TEST_TXFILE, raw.toString('base64') + '\n');
    return `signature ${tx.signatures[0].signature && (await import('bs58')).default.encode(tx.signatures[0].signature)}, ${raw.length} bytes`;
  });
  await step('devnet sendRawTransaction', async () => devnet.sendRawTransaction(tx.serialize()));
}

// web3.js keeps its PubSub socket open; end the process.
setTimeout(() => process.exit(0), 500);
