package com.minirabbit.core;

public final class Routing {
  private Routing() {}

  /** Dynamic programming: O(pattern segments * key segments), no recursive wildcard explosion. */
  public static boolean topic(String pattern, String key) {
    String[] p = pattern.isEmpty() ? new String[0] : pattern.split("\\.", -1),
        k = key.isEmpty() ? new String[0] : key.split("\\.", -1);
    boolean[][] dp = new boolean[p.length + 1][k.length + 1];
    dp[0][0] = true;
    for (int i = 1; i <= p.length; i++)
      for (int j = 0; j <= k.length; j++)
        dp[i][j] =
            p[i - 1].equals("#")
                ? (dp[i - 1][j] || j > 0 && dp[i][j - 1])
                : j > 0 && dp[i - 1][j - 1] && (p[i - 1].equals("*") || p[i - 1].equals(k[j - 1]));
    return dp[p.length][k.length];
  }
}
