// Build a 1-lamport devnet transfer from the fresh test key, with a recent
// blockhash fetched through the gateway, and print it base64 (signed, or
// unsigned with --unsigned). The key is never printed.
//
//   node --require ./no-bypass-hook.cjs sign-transfer.mjs KEYFILE [--unsigned] | solanamix send-raw - --network devnet
import { readFileSync } from 'node:fs';
import { Connection, Keypair, SystemProgram, Transaction } from '@solana/web3.js';

const kp = Keypair.fromSecretKey(Uint8Array.from(JSON.parse(readFileSync(process.argv[2]))));
const unsigned = process.argv.includes('--unsigned');
const devnet = new Connection(process.env.SOLANAMIX_RPC || 'http://127.0.0.1:8899/devnet', 'confirmed');
const { blockhash } = await devnet.getLatestBlockhash();
const tx = new Transaction({ feePayer: kp.publicKey, recentBlockhash: blockhash })
  .add(SystemProgram.transfer({ fromPubkey: kp.publicKey, toPubkey: Keypair.generate().publicKey, lamports: 1 }));
if (!unsigned) tx.sign(kp);
process.stdout.write(tx.serialize({ requireAllSignatures: !unsigned, verifySignatures: !unsigned }).toString('base64') + '\n');
process.exit(0);
