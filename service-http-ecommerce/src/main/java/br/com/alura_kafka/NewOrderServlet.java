package br.com.alura_kafka;

import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>();


    @Override
    public void destroy() {
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            String orderId = req.getParameter("uuid");
            BigDecimal amount = new BigDecimal(req.getParameter("amount"));
            String email = req.getParameter("email");

            try (var database = new OrdersDatabase()) {
                Order order = new Order(orderId, amount, email);

                if (database.saveNew(order)) {
                    var id = new CorrelationId(NewOrderServlet.class.getSimpleName());
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
                    log.info("new order send");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order send \nEmail:" + email + " \nAmount: " + amount);
                } else {
                    System.out.println("Old order received");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received");
                }
            }


        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
