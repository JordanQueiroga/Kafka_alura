package br.com.alura_kafka;

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
    private final KafkaDispatcher emailDispatcher = new KafkaDispatcher<String>();


    @Override
    public void destroy() {
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            String orderId = UUID.randomUUID().toString();
            BigDecimal amount = new BigDecimal(req.getParameter("amount"));
            String email = req.getParameter("email");

            Order order = new Order(orderId, amount, email);

            var sendEmailValue = "Thank for your order!";

            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), sendEmailValue);
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
