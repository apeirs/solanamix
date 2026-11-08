// Create a fresh, never-funded devnet key for the send test. Prints the address only.
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { Keypair } from '@solana/web3.js';

const out = process.argv[2];
if (existsSync(out)) {
  const kp = Keypair.fromSecretKey(Uint8Array.from(JSON.parse(readFileSync(out))));
  console.log('exists, address', kp.publicKey.toBase58());
  process.exit(0);
}
const kp = Keypair.generate();
// Same format as solana-keygen (a JSON array of 64 bytes).
writeFileSync(out, JSON.stringify(Array.from(kp.secretKey)), { mode: 0o600 });
console.log('new throwaway devnet address', kp.publicKey.toBase58());
