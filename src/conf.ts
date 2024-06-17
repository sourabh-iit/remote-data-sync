export const getEnvVariable = (varName: string): string => {
  const value = process.env[varName];
  if(!value) {
    throw new Error(`Evn variable ${varName} is required`);
  }
  return value;
}

export const CHUNK_SIZE = 32;
export const CHECKPOINT_STORAGE = "FILE";