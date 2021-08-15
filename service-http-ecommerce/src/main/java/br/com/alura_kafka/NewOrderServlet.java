package br.com.alura_kafka;

import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
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
            String orderId = UUID.randomUUID().toString();
            BigDecimal amount = new BigDecimal(req.getParameter("amount"));
            String email = req.getParameter("email");

            Order order = new Order(orderId, amount, email);
            var id = new CorrelationId(NewOrderServlet.class.getSimpleName());

            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
            log.info("new order send");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order send \nEmail:" + email + " \nAmount: " + amount);

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
