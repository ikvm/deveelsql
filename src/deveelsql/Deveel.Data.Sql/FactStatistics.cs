using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql {
	public class FactStatistics {
		private SystemTransaction transaction;
		private readonly Dictionary<String, FactSamples> fact_map;

		internal FactStatistics(SystemTransaction transaction) {
			this.transaction = transaction;
			fact_map = new Dictionary<string, FactSamples>();
		}

		public static void ToFactString(Expression expression, StringBuilder sb) {
			if (expression is FetchVariableExpression) {
				sb.Append(((FetchVariableExpression)expression).Variable);
				sb.Append("||");
			} else if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;
				Expression p0 = (Expression) functionExp.Parameters[0];
				Expression p1 = (Expression) functionExp.Parameters[1];
				ToFactString(p0, sb);
				ToFactString(p1, sb);
				sb.Append(functionExp.Name);
				sb.Append("|");
			} else {
				throw new ApplicationException("Unexpected operation");
			}
		}

		public static bool CanBeFact(Expression expression) {
			if (expression is FetchVariableExpression)
				return true;

			if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;
				string fun_type = functionExp.Name;
				if (QueryPlanner.IsSimpleLogical(fun_type) ||
					QueryPlanner.IsSimpleComparison(fun_type) ||
					QueryPlanner.IsSimpleArithmetic(fun_type)) {
					return CanBeFact((Expression) functionExp.Parameters[0]) && CanBeFact((Expression) functionExp.Parameters[1]);
				}
				
				return false;
			}
				
			return false;
		}

		public static String ToFactId(Expression op) {
			// The operation is converted into a simple left deep postfix format where
			// the terms are ordered alphebetically.
			StringBuilder buf = new StringBuilder();
			ToFactString(op, buf);
			return buf.ToString();
		}

		public double ProbabilityEstimate(String fact_id) {
			FactSamples fact_samples;
			if (!fact_map.TryGetValue(fact_id, out fact_samples)) {
				return 1.0d;
			}
			return fact_samples.Average;
		}

		public int FactSampleCount(String fact_id) {
			FactSamples fact_samples;
			if (!fact_map.TryGetValue(fact_id, out fact_samples)) {
				return 0;
			}
			return fact_samples.Count;
		}

		public void AddFactSample(String fact_id, double probability) {
			FactSamples fact_samples;
			if (!fact_map.TryGetValue(fact_id, out fact_samples)) {
				fact_samples = new FactSamples();
				fact_map[fact_id] = fact_samples;
			}
			fact_samples.Add(probability);
		}


		#region FactSamples

		private sealed class FactSamples {
			// The samples are recorded in an array that wraps around
			private readonly double[] samples;
			private int p;
			private int count;

			private double average, min, max;

			public FactSamples() {
				samples = new double[16];
				p = 0;
				count = 0;
			}

			// Total number of samples recorded (can't be greater than samples.length)
			public int Count {
				get { return count; }
			}

			// Average of the samples recorded
			public double Average {
				get { return average; }
			}

			// Min of the samples recorded
			public double Min {
				get { return min; }
			}

			// Max of the samples recorded
			public double Max {
				get { return max; }
			}

			// Adds a probability to the list of samples.
			public void Add(double probability) {
				samples[p] = probability;
				++p;
				if (count < samples.Length) {
					++count;
				}
				if (p >= samples.Length) {
					p = 0;
				}
				Update();
			}

			// Updates stats
			private void Update() {
				double total = 0;
				max = Double.NegativeInfinity;
				min = Double.PositiveInfinity;
				int s = p - count;
				if (s < 0) {
					s = s + samples.Length;
				}

				for (int n = 0; n < samples.Length; ++n) {
					double sample = samples[s + n];
					max = System.Math.Max(max, sample);
					min = System.Math.Min(min, sample);
					total = total + sample;
				}

				average = (total / (double)samples.Length);
			}
		}

		#endregion
	}
}