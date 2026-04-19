/**
 * Niveles de severidad para el control de logs.
 */
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  SILENT = 4,
}

/**
 * Logger de Grado Militar - Cero uso de 'any'.
 */
class Logger {
  private level: LogLevel = LogLevel.INFO;

  /**
   * Ajusta la verbosidad del motor en tiempo de ejecución.
   */
  public setLevel(level: LogLevel): void {
    this.level = level;
  }

  /**
   * Reporta información general del sistema.
   */
  public info(message: string, ...meta: unknown[]): void {
    if (this.level <= LogLevel.INFO) {
      console.info(`[CERVID-INFO] ${message}`, ...meta);
    }
  }

  /**
   * Reporta advertencias que no detienen la ejecución pero requieren atención.
   */
  public warn(message: string, ...meta: unknown[]): void {
    if (this.level <= LogLevel.WARN) {
      console.warn(`[CERVID-WARN] ${message}`, ...meta);
    }
  }

  /**
   * Reporta errores críticos del motor o de datos.
   */
  public error(message: string, error?: Error | unknown): void {
    if (this.level <= LogLevel.ERROR) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`[CERVID-ERROR] ${message}`, errorMessage);
    }
  }

  /**
   * Reporta detalles técnicos útiles para el desarrollo.
   */
  public debug(message: string, ...meta: unknown[]): void {
    if (this.level <= LogLevel.DEBUG) {
      console.debug(`[CERVID-DEBUG] ${message}`, ...meta);
    }
  }
}

// Exportamos la instancia única (Singleton) para mantener el estado del nivel de log
export const logger = new Logger();
