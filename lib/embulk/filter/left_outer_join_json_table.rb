Embulk::JavaPlugin.register_filter(
  "left_outer_join_json_table", "org.embulk.filter.left_outer_join_json_table.LeftOuterJoinJsonTableFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
