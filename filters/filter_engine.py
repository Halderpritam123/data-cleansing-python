# filters/filter_engine.py

from pandasql import sqldf

class FilterEngine:
    def __init__(self, dataframe):
        self.original_df = dataframe.copy()

        # Clean column names to be SQL-safe
        self.cleaned_df = self.original_df.copy()
        self.col_map = {}
        for col in self.cleaned_df.columns:
            clean_col = col.strip().replace(" ", "_").replace("-", "_")
            if clean_col.lower() == "index":
                clean_col = "index_col"
            self.col_map[col] = clean_col

        self.cleaned_df.columns = list(self.col_map.values())

    def apply_filter(self, selected_columns, sql_condition=None):
        try:
            # Translate selected_columns to cleaned names
            cleaned_cols = [self.col_map.get(col, col) for col in selected_columns]
            query = f"SELECT {', '.join(cleaned_cols)} FROM self_df"
            if sql_condition:
                query += f" WHERE {sql_condition}"

            result_df = sqldf(query, {"self_df": self.cleaned_df})

            # Restore original column names
            reverse_col_map = {v: k for k, v in self.col_map.items()}
            result_df.columns = [reverse_col_map.get(c, c) for c in result_df.columns]

            # Dropped columns
            dropped_cols = [col for col in self.original_df.columns if col not in selected_columns]
            dropped_df = self.original_df[dropped_cols]

            return result_df, dropped_df

        except Exception as e:
            raise Exception(f"Filter processing failed: {e}")
