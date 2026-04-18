import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    // 1. Ignorar archivos que no son lógica de negocio
    ignores: [
      'dist/**', 
      'node_modules/**', 
      'archive/**', 
      '**/*.d.ts', // 🛡️ Evita el error de parsing en archivos de definición
      'eslint.config.js'
    ],
  },
  {
    files: ['src/**/*.ts'], // Solo auditar el código fuente
    languageOptions: {
      parserOptions: {
        project: './tsconfig.json',
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-unused-vars': ['error', { 
        'argsIgnorePattern': '^_',
        'varsIgnorePattern': '^_'
      }],
      '@typescript-eslint/no-floating-promises': 'error',
      'no-console': ['warn', { allow: ['info', 'warn', 'error', 'debug', 'table'] }],
      'prefer-const': 'error',
    // 🛡️ Prohíbe tipos "vacíos" o interfaces inútiles
      '@typescript-eslint/no-empty-interface': 'error',

      // 🛡️ Obliga a ser explícito en los valores de retorno de funciones públicas
      // (Así cualquier desarrollador sabe qué devuelve un método sin mirar el código)
      '@typescript-eslint/explicit-module-boundary-types': 'error',

      // 🛡️ Prohíbe el uso de 'require' (fuerza el uso de ESM / import)
      '@typescript-eslint/no-var-requires': 'error',

      // 🛡️ Evita comparaciones confusas que podrían dar falsos positivos en los filtros
      'no-constant-condition': 'error',

      // 🛡️ Detecta código que nunca se ejecutará (Dead Code avanzado)
      'no-unreachable': 'error',
    },
  }
);