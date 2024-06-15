using System.Data;
using System.Data.Common;
using Northwind.Services.Repositories;

namespace Northwind.Services.Ado.Repositories
{
    public sealed class OrderRepository : IOrderRepository
    {
        private readonly DbConnection connection;

        public OrderRepository(DbProviderFactory dbFactory, string connectionString)
        {
            if (dbFactory is not null)
            {
                this.connection = dbFactory.CreateConnection() ?? throw new ArgumentNullException(nameof(dbFactory));
                this.connection.ConnectionString = connectionString;
                this.connection.Open();
            }
            else
            {
                throw new ArgumentNullException(nameof(dbFactory));
            }
        }

        public async Task<IList<Order>> GetOrdersAsync(int skip, int count)
        {
            OrderRepositoryHelper.ValidateGetOrdersParamsAndConnection(this.connection, skip, count);

            IList<Order> orders = new List<Order>();

            try
            {
                using var command = this.connection.CreateCommand();
                command.CommandText = "SELECT OrderId FROM Orders ORDER BY OrderId LIMIT @Take OFFSET @Skip";

                OrderRepositoryHelper.AddParameter(command, "@Skip", skip);

                OrderRepositoryHelper.AddParameter(command, "@Take", count);

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    orders.Add(new Order(reader.GetInt64(0)));
                }
            }
            catch (Exception)
            {
                throw;
            }

            this.connection.Close();
            return orders;
        }

        public async Task<Order> GetOrderAsync(long orderId)
        {
            if (this.connection.State.HasFlag(ConnectionState.Closed))
            {
                this.connection.Open();
            }

            Order? order = null;

            try
            {
                using var command = this.connection.CreateCommand();
                command.CommandText = $"SELECT COUNT(*) FROM Orders WHERE OrderID = @OrderId";
                OrderRepositoryHelper.AddParameter(command, "@OrderId", orderId);

                object? result = command.ExecuteScalar();

                if (result is null || (long)result is 0)
                {
                    throw new RepositoryException($"No such record with {orderId}");
                }

                command.Parameters.Clear();

                command.CommandText = "SELECT Orders.*," +
                                      " Customers.CompanyName AS CustomerCompanyName," +
                                      " Employees.FirstName, Employees.LastName, Employees.Country," +
                                      " Shippers.CompanyName AS ShipperCompanyName" +
                                      " FROM Orders" +
                                      " JOIN Customers ON Orders.CustomerId = Customers.CustomerId" +
                                      " JOIN Employees ON Orders.EmployeeId = Employees.EmployeeId" +
                                      " JOIN Shippers ON Orders.ShipVia = Shippers.ShipperId" +
                                      " WHERE OrderId = @Id";

                var id = command.CreateParameter();
                id.ParameterName = "@Id";
                id.Value = orderId;
                _ = command.Parameters.Add(id);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        order = new Order(reader.GetInt64(0))
                        {
                            Customer = new Customer(new CustomerCode(reader.GetString(1)))
                            {
                                CompanyName = (string)reader["CustomerCompanyName"],
                            },
                            Employee = new Employee(reader.GetInt64(2))
                            {
                                FirstName = (string)reader["FirstName"],
                                LastName = (string)reader["LastName"],
                                Country = (string)reader["Country"],
                            },
                            Shipper = new Shipper(reader.GetInt64(6))
                            {
                                CompanyName = (string)reader["ShipperCompanyName"],
                            },
                            ShippingAddress = new ShippingAddress(reader.GetString(9), reader.GetString(10), reader.IsDBNull(11) ? null : reader.GetString(11), reader.GetString(12), reader.GetString(13)),
                            OrderDate = DateTime.Parse(reader.GetString(3), System.Globalization.CultureInfo.InvariantCulture),
                            RequiredDate = DateTime.Parse(reader.GetString(4), System.Globalization.CultureInfo.InvariantCulture),
                            ShippedDate = DateTime.Parse(reader.GetString(5), System.Globalization.CultureInfo.InvariantCulture),
                            Freight = reader.GetDouble(7),
                            ShipName = reader.GetString(8),
                        };
                    }
                }

                command.CommandText = "SELECT OrderDetails.UnitPrice, OrderDetails.Quantity, OrderDetails.Discount," +
                                      " Products.ProductId, Products.ProductName, Products.CategoryId, Products.SupplierId," +
                                      " Suppliers.CompanyName, Categories.CategoryName FROM OrderDetails" +
                                      " JOIN Products ON OrderDetails.ProductId = Products.ProductId" +
                                      " JOIN Suppliers ON Products.SupplierId = Suppliers.SupplierId" +
                                      " JOIN Categories ON Products.CategoryId = Categories.CategoryId" +
                                      " WHERE OrderDetails.OrderId = @Id";

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        order!.OrderDetails.Add(new OrderDetail(order)
                        {
                            Product = new Product((long)reader["ProductId"])
                            {
                                ProductName = (string)reader["ProductName"],
                                CategoryId = (long)reader["CategoryId"],
                                Category = (string)reader["CategoryName"],
                                SupplierId = (long)reader["SupplierId"],
                                Supplier = (string)reader["CompanyName"],
                            },
                            UnitPrice = (double)reader["UnitPrice"],
                            Quantity = (long)reader["Quantity"],
                            Discount = (double)reader["Discount"],
                        });
                    }
                }
            }
            catch (RepositoryException)
            {
                this.connection.Close();
                throw;
            }
            catch (Exception)
            {
                // Handle exception
                throw;
            }

            this.connection.Close();
            return order!;
        }

        public async Task<long> AddOrderAsync(Order order)
        {
            if (this.connection.State.HasFlag(ConnectionState.Closed))
            {
                this.connection.Open();
            }

            if (!OrderRepositoryHelper.ValidateOrder(order))
            {
                this.connection.Close();
                throw new RepositoryException("Order");
            }

            using (var transaction = this.connection.BeginTransaction())
            {
                try
                {
                    using DbCommand command = this.connection.CreateCommand();

                    command.CommandText = $"SELECT COUNT(*) FROM Orders WHERE OrderID = @OrderId";
#pragma warning disable CA1062 // Validate arguments of public methods
                    OrderRepositoryHelper.AddParameter(command, "@OrderId", order.Id);
#pragma warning restore CA1062 // Validate arguments of public methods
                    object? result = command.ExecuteScalar();
                    if (result is null || (long)result is not 0)
                    {
                        this.connection.Close();
                        return order.Id;
                    }

                    command.Parameters.Clear();

                    command.Transaction = transaction;
                    command.CommandText = "INSERT INTO Orders VALUES (@Id, @Customer, @Employee, @OrderDate, @RequiredDate, @ShippedDate, @ShipVia, @Freight, @ShipName, @ShipAddress, @ShipCity, @ShipRegion, @ShipPostalCode, @ShipCountry)";

                    OrderRepositoryHelper.AddParameter(command, "@Id", order.Id);
                    OrderRepositoryHelper.AddParameter(command, "@Customer", order.Customer.Code.Code);
                    OrderRepositoryHelper.AddParameter(command, "@Employee", order.Employee.Id);
                    OrderRepositoryHelper.AddParameter(command, "@OrderDate", order.OrderDate);
                    OrderRepositoryHelper.AddParameter(command, "@RequiredDate", order.RequiredDate);
                    OrderRepositoryHelper.AddParameter(command, "@ShippedDate", order.ShippedDate);
                    OrderRepositoryHelper.AddParameter(command, "@ShipVia", order.Shipper.Id);
                    OrderRepositoryHelper.AddParameter(command, "@Freight", order.Freight);
                    OrderRepositoryHelper.AddParameter(command, "@ShipName", order.ShipName);
                    OrderRepositoryHelper.AddParameter(command, "@ShipAddress", order.ShippingAddress.Address);
                    OrderRepositoryHelper.AddParameter(command, "@ShipCity", order.ShippingAddress.City);
                    OrderRepositoryHelper.AddParameter(command, "@ShipRegion", order.ShippingAddress.Region != null ? order.ShippingAddress.Region : DBNull.Value);
                    OrderRepositoryHelper.AddParameter(command, "@ShipPostalCode", order.ShippingAddress.PostalCode);
                    OrderRepositoryHelper.AddParameter(command, "@ShipCountry", order.ShippingAddress.Country);

                    try
                    {
                        _ = await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception)
                    {
                        throw;
                    } // insert Order

                    IList<OrderDetail> orderDetails = order.OrderDetails;

                    foreach (OrderDetail orderDetail in orderDetails)
                    {
                        DbCommand cmd = this.connection.CreateCommand();
                        cmd.Transaction = transaction;
                        cmd.CommandText = "INSERT INTO OrderDetails VALUES (@Order, @Product, @UnitPrice, @Quantity, @Discount)";

                        OrderRepositoryHelper.AddParameter(cmd, "@Order", orderDetail.Order.Id);
                        OrderRepositoryHelper.AddParameter(cmd, "@Product", orderDetail.Product.Id);
                        OrderRepositoryHelper.AddParameter(cmd, "@UnitPrice", orderDetail.UnitPrice);
                        OrderRepositoryHelper.AddParameter(cmd, "@Quantity", orderDetail.Quantity);
                        OrderRepositoryHelper.AddParameter(cmd, "@Discount", orderDetail.Discount);

                        try
                        {
                            _ = await cmd.ExecuteNonQueryAsync();
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                    }

                    transaction.Commit();
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }

            this.connection.Close();
            return order.Id;
        }

        public async Task RemoveOrderAsync(long orderId)
        {
            if (this.connection.State.HasFlag(ConnectionState.Closed))
            {
                this.connection.Open();
            }

            try
            {
                using var command = this.connection.CreateCommand();
                command.CommandText = "DELETE FROM OrderDetails WHERE OrderId = @Id";

                OrderRepositoryHelper.AddParameter(command, "@Id", orderId);

                _ = await command.ExecuteNonQueryAsync();

                command.CommandText = "DELETE FROM Orders WHERE OrderId = @Id";

                _ = await command.ExecuteNonQueryAsync();
            }
            catch (Exception)
            {
                throw;
            }

            this.connection.Close();
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task UpdateOrderAsync(Order order)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (this.connection.State.HasFlag(ConnectionState.Closed))
            {
                this.connection.Open();
            }

            using (var transaction = this.connection.BeginTransaction())
            {
                try
                {
                    using (var command = this.connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT COUNT(*) FROM Orders WHERE OrderID = @OrderId";
#pragma warning disable CA1062 // Validate arguments of public methods
                        OrderRepositoryHelper.AddParameter(command, "@OrderId", order.Id);
#pragma warning restore CA1062 // Validate arguments of public methods
                        object? result = command.ExecuteScalar();
                        if (result is null || (long)result is 0)
                        {
                            throw new RepositoryException($"No such record with {order.Id}");
                        }

                        command.Parameters.Clear();

                        command.Transaction = transaction;
                        command.CommandText = "UPDATE Orders SET CustomerId = @CustomerId, EmployeeId = @EmployeeId, OrderDate = @OrderDate," +
                                                " RequiredDate = @RequiredDate, ShippedDate = @ShippedDate, ShipVia = @ShipVia, Freight = @Freight, ShipName = @ShipName," +
                                                " ShipAddress = @ShipAddress, ShipCity = @ShipCity, ShipRegion = @ShipRegion, ShipPostalCode = @ShipPostalCode, ShipCountry = @ShipCountry" +
                                                " WHERE OrderId = @Id";

                        OrderRepositoryHelper.AddParameter(command, "@Id", order.Id);
                        OrderRepositoryHelper.AddParameter(command, "@CustomerId", order.Customer.Code.Code);
                        OrderRepositoryHelper.AddParameter(command, "@EmployeeId", order.Employee.Id);
                        OrderRepositoryHelper.AddParameter(command, "@OrderDate", order.OrderDate);
                        OrderRepositoryHelper.AddParameter(command, "@RequiredDate", order.RequiredDate);
                        OrderRepositoryHelper.AddParameter(command, "@ShippedDate", order.ShippedDate);
                        OrderRepositoryHelper.AddParameter(command, "@ShipVia", order.Shipper.Id);
                        OrderRepositoryHelper.AddParameter(command, "@Freight", order.Freight);
                        OrderRepositoryHelper.AddParameter(command, "@ShipName", order.ShipName);
                        OrderRepositoryHelper.AddParameter(command, "@ShipAddress", order.ShippingAddress.Address);
                        OrderRepositoryHelper.AddParameter(command, "@ShipCity", order.ShippingAddress.City);
                        OrderRepositoryHelper.AddParameter(command, "@ShipRegion", order.ShippingAddress.Region != null ? order.ShippingAddress.Region : DBNull.Value);
                        OrderRepositoryHelper.AddParameter(command, "@ShipPostalCode", order.ShippingAddress.PostalCode);
                        OrderRepositoryHelper.AddParameter(command, "@ShipCountry", order.ShippingAddress.Country);

                        _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                        command.Parameters.Clear();

                        command.CommandText = "UPDATE Customers SET CompanyName = @CompanyName" +
                                                " WHERE CustomerId = @Id";

                        OrderRepositoryHelper.AddParameter(command, "@Id", order.Customer.Code.Code);
                        OrderRepositoryHelper.AddParameter(command, "@CompanyName", order.Customer.CompanyName);
                        _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                        command.Parameters.Clear();

                        command.CommandText = "UPDATE Employees SET FirstName = @FirstName, LastName = @LastName," +
                                                " Country = @Country" +
                                                " WHERE EmployeeId = @Id";

                        OrderRepositoryHelper.AddParameter(command, "@Id", order.Employee.Id);
                        OrderRepositoryHelper.AddParameter(command, "@FirstName", order.Employee.FirstName);
                        OrderRepositoryHelper.AddParameter(command, "@LastName", order.Employee.LastName);
                        OrderRepositoryHelper.AddParameter(command, "@Country", order.Employee.Country);
                        _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                        command.Parameters.Clear();

                        command.CommandText = "UPDATE Shippers SET CompanyName = @CompanyName" +
                                                " WHERE ShipperId = @Id";

                        OrderRepositoryHelper.AddParameter(command, "@Id", order.Shipper.Id);
                        OrderRepositoryHelper.AddParameter(command, "@CompanyName", order.Shipper.CompanyName);
                        _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                        command.Parameters.Clear();

                        IList<long> productIdsArray = new List<long>();

                        command.CommandText = "DELETE FROM OrderDetails WHERE OrderId = @Id";
                        OrderRepositoryHelper.AddParameter(command, "@Id", order.Id);
                        _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                        command.Parameters.Clear();

                        foreach (OrderDetail o in order.OrderDetails)
                        {
                            productIdsArray.Add(o.Product.Id);

                            command.CommandText = "INSERT INTO OrderDetails VALUES (@OrderId, @ProductId, @UnitPrice," +
                                                    "@Quantity," +
                                                    "@Discount)";

                            OrderRepositoryHelper.AddParameter(command, "@OrderId", order.Id);
                            OrderRepositoryHelper.AddParameter(command, "@ProductId", o.Product.Id);
                            OrderRepositoryHelper.AddParameter(command, "@UnitPrice", o.UnitPrice);
                            OrderRepositoryHelper.AddParameter(command, "@Discount", o.Discount);
                            OrderRepositoryHelper.AddParameter(command, "@Quantity", o.Quantity);
                            _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                            command.Parameters.Clear();

                            command.CommandText = "UPDATE Products SET ProductName = @ProductName" +
                                                    " WHERE ProductId = @Id";

                            OrderRepositoryHelper.AddParameter(command, "@Id", o.Product.Id);
                            OrderRepositoryHelper.AddParameter(command, "@ProductName", o.Product.ProductName);
                            _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                            command.Parameters.Clear();

                            command.CommandText = "UPDATE Suppliers SET CompanyName = @CompanyName" +
                                                    " WHERE SupplierId = @Id";

                            OrderRepositoryHelper.AddParameter(command, "@Id", o.Product.SupplierId);
                            OrderRepositoryHelper.AddParameter(command, "@CompanyName", o.Product.Supplier);
                            _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                            command.Parameters.Clear();

                            command.CommandText = "UPDATE Categories SET CategoryName = @CategoryName" +
                                                    " WHERE CategoryId = @Id";

                            OrderRepositoryHelper.AddParameter(command, "@Id", o.Product.CategoryId);
                            OrderRepositoryHelper.AddParameter(command, "@CategoryName", o.Product.Category);
                            _ = OrderRepositoryHelper.ExecuteNonQueryAsync(command);
                            command.Parameters.Clear();
                        }
                    }

                    transaction.Commit();
                }
                catch (RepositoryException rex)
                {
                    this.connection.Close();
                    throw new RepositoryException(rex.Message, rex);
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }

            this.connection.Close();
        }
    }
}
