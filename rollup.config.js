import resolve from 'rollup-plugin-node-resolve'
import { terser } from 'rollup-plugin-terser'
import cleanup from 'rollup-plugin-cleanup'

export default {
  input: 'src/index.js',
  external: [ 'fs', 'util' ],
  plugins: [
    resolve(),
    cleanup(),
    process.env.NODE_ENV === 'production' && terser()
  ],
  output: [
    {
      file: 'dist/index.js',
      format: 'cjs',
      sourcemap: true,
    },
    {
      file: 'dist/index.mjs',
      format: 'esm',
      sourcemap: true
    }
  ]
}
