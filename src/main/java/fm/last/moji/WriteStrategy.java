package fm.last.moji;

/**
 * File write strategies. Effect file durability and file write performance. Note that the strategy only take effect on
 * file write procedure, and have nothing to do with number of replicas belong to a file in the end. (It is decided by
 * storage class.)
 */
public enum WriteStrategy {
  /**
   * Classical way, store just one copy when uploading file.
   */
  DEFAULT,
  /**
   * A more durable way, the file will have at least two replicas before file write is acknowledged. Caution that
   * this way consumes more bandwidth and may cause performance degradation.
   */
  DURABLE
}
