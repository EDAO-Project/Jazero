package dk.aau.cs.dkwe.edao.calypso.knowledgegraph;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.hamcrest.Matchers.equalTo;

@SpringBootTest
@AutoConfigureMockMvc
public class EKGManagerTest
{
    @Autowired
    private MockMvc mvc;

    @Test
    public void testPing() throws Exception
    {
        this.mvc.perform(MockMvcRequestBuilders.get("/ping").accept(MediaType.TEXT_PLAIN))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_PLAIN))
                .andExpect(content().string(equalTo("pong")));
    }

    @Test
    public void testGetKG() throws Exception
    {
        this.mvc.perform(MockMvcRequestBuilders.get("/get-kg"))
                                                .andExpect(status().isOk())
                                                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testInsertLinks() throws Exception
    {
        this.mvc.perform(MockMvcRequestBuilders.post("/insert-links")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"folder\": \"test_folder\"}"))
                .andExpect(status().isOk());
    }
}
