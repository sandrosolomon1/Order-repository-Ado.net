using System.Data;
using System.Data.Common;
using Northwind.Services.Repositories;

namespace Northwind.Services.Ado.Repositories
{
    public sealed class OrderRepositoryHelper
    {
        public static void AddParameter(DbCommand cmd, string name, object value)
        {
            if (cmd is not null)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = name;
                param.Value = value;
                _ = cmd.Parameters.Add(param);
            }
        }

        public static async Task ExecuteNonQueryAsync(DbCommand cmd)
        {
            try
            {
                if (cmd is not null)
                {
                    _ = await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool ValidateOrder(Order order)
        {
            bool isValid = order is not null &&
                           order.Id > 0 &&
                           order.Customer != null &&
                           order.Employee != null;

            if (isValid)
            {
                if (order is null)
                {
                    return false;
                }

                foreach (var orderDetail in order.OrderDetails)
                {
                    isValid = orderDetail.Order != null && orderDetail.Order.Id > 0 &&
                              orderDetail.Product != null && orderDetail.Product.Id > 0 &&
                              orderDetail.UnitPrice > 0 &&
                              orderDetail.Quantity > 0 &&
                              orderDetail.Discount >= 0;

                    if (!isValid)
                    {
                        break;
                    }
                }
            }

            return isValid;
        }

        public static void ValidateGetOrdersParamsAndConnection(DbConnection conn, int skip, int count)
        {
            if (conn is not null && conn.State.HasFlag(ConnectionState.Closed))
            {
                conn.Open();
            }

            if ((skip < 1 && count < 1) || (skip < 0 || count < 0))
            {
                throw new ArgumentOutOfRangeException(nameof(skip));
            }
        }
    }
}
