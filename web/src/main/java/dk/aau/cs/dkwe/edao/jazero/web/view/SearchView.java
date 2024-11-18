package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Html;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.html.Image;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.*;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.internal.Pair;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.connector.DataLakeService;
import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.jazero.web.util.ConfigReader;
import dk.aau.cs.dkwe.edao.structures.Query;
import dk.aau.cs.dkwe.edao.structures.TableQuery;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Route(value = "")
public class SearchView extends Div
{
    private final VerticalLayout layout = new VerticalLayout();
    private final List<List<StringBuilder>> query = new ArrayList<>();
    private final Section querySection = new Section();
    private final Grid<Pair<String, Integer>> entityCounts = new Grid<>();
    private final List<dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair<File, Double>> results = new ArrayList<>();
    private Component resultComponent = null;
    private static int TEXTFIELD_WIDTH = 300;
    private static final int TEXTFIELD_HEIGHT = 25;

    public SearchView()
    {
        this.layout.setDefaultHorizontalComponentAlignment(FlexComponent.Alignment.CENTER);

        Component header = buildHeader(), searchBar = buildSearchBar();
        this.layout.add(header, searchBar);
        add(this.layout);
        getStyle().set("background-color", "#FEC69B");
        setHeightFull();
    }

    private Component buildHeader()
    {
        HorizontalLayout header = new HorizontalLayout();
        header.setWidthFull();
        header.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        header.setAlignItems(FlexComponent.Alignment.CENTER);
        header.getStyle().set("padding", "5px");
        header.getStyle().set("border-bottom", "1px solid #ccc");
        header.getStyle().set("background-image", "url('/images/cover.png')");
        header.getStyle().set("background-size", "cover");
        header.getStyle().set("height", "150px");
        header.getStyle().set("position", "fixed");
        header.getStyle().set("top", "0");
        header.getStyle().set("left", "0");
        header.getStyle().set("right", "0");

        Image logo = new Image("images/logo.png", "Jazero Logo");
        logo.setHeight("100px");

        Div systemName = new Div();
        systemName.setText("Jazero");
        systemName.getStyle().set("font-size", "100px");
        systemName.getStyle().set("font-weight", "bold");
        systemName.getStyle().set("margin-left", "10px");
        systemName.getStyle().set("color", "white");
        header.add(logo, systemName);

        return header;
    }

    private Component buildSearchBar()
    {
        Component tableInput = buildTableQueryInputComponent();
        HorizontalLayout queryTableLayout = new HorizontalLayout(tableInput);
        queryTableLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        queryTableLayout.setAlignItems(FlexComponent.Alignment.CENTER);

        Component actionComponent = buildSearchComponent();

        VerticalLayout searchBarLayout = new VerticalLayout(queryTableLayout, actionComponent);
        searchBarLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        searchBarLayout.setAlignItems(FlexComponent.Alignment.CENTER);
        searchBarLayout.getStyle().set("margin-top", "200px");

        return searchBarLayout;
    }

    private Component buildTableQueryInputComponent()
    {
        int rows = 3, columns = 3;
        FlexLayout tableLayout = new FlexLayout();
        H2 label = new H2("Input Query");
        HorizontalLayout addColumnLayout = new HorizontalLayout();
        VerticalLayout addRowLayout = new VerticalLayout();
        tableLayout.setFlexWrap(FlexLayout.FlexWrap.WRAP);
        addColumnLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        addColumnLayout.setAlignItems(FlexComponent.Alignment.CENTER);
        label.addClassNames(LumoUtility.FontWeight.BOLD);

        for (int i = 0; i < rows; i++)
        {
            List<StringBuilder> column = new ArrayList<>();

            for (int j = 0; j < columns; j++)
            {
                column.add(new StringBuilder());
            }

            this.query.add(column);
        }

        buildQueryTable();

        Scroller queryScroller = new Scroller(new Div(this.querySection));
        queryScroller.getStyle().set("border", "2px solid #191817");
        queryScroller.setWidth(1.08 * columns * TEXTFIELD_WIDTH + "px");
        queryScroller.setHeight(2.35 * rows * TEXTFIELD_HEIGHT + "px");

        Icon plusIconAddRow = new Icon(VaadinIcon.PLUS), minusIconRemoveRow = new Icon(VaadinIcon.MINUS),
                plusIconAddColumn = new Icon(VaadinIcon.PLUS), minusIconRemoveColumn = new Icon(VaadinIcon.MINUS);
        plusIconAddRow.setColor("#3D423F");
        minusIconRemoveRow.setColor("#3D423F");
        plusIconAddColumn.setColor("#3D423F");
        minusIconRemoveColumn.setColor("#3D423F");

        HorizontalLayout changeRowLayout = new HorizontalLayout();
        Button addRowButton = new Button(plusIconAddRow, item -> addRow());
        Button removeRowButton = new Button(minusIconRemoveRow, item -> removeRow());
        addRowButton.setWidth(((columns * TEXTFIELD_WIDTH) * 2 / 3) + "px");
        addRowButton.getStyle().set("background-color", "#ABB6B1");
        removeRowButton.setWidth(((columns * TEXTFIELD_WIDTH) / 3) + "px");
        removeRowButton.getStyle().set("background-color", "#ABB6B1");
        changeRowLayout.getStyle().set("margin-top", "-10px");
        changeRowLayout.add(addRowButton, removeRowButton);
        changeRowLayout.setAlignItems(FlexComponent.Alignment.CENTER);
        changeRowLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        changeRowLayout.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.AlignContent.CENTER,
                LumoUtility.JustifyContent.CENTER);

        VerticalLayout changeColumnLayout = new VerticalLayout();
        Button addColumnButton = new Button(plusIconAddColumn, item -> addColumn());
        Button removeColumnButton = new Button(minusIconRemoveColumn, item -> removeColumn());
        VerticalLayout entityCountsLayout = new VerticalLayout();
        H3 entityCountsLabel = new H3("Entity counts");
        entityCountsLabel.addClassNames(LumoUtility.FontWeight.BOLD);
        addColumnButton.setHeight(((TEXTFIELD_HEIGHT * rows) * 2 / 3) + "px");
        addColumnButton.getStyle().set("background-color", "#ABB6B1");
        removeColumnButton.setHeight(((rows * TEXTFIELD_HEIGHT) / 3) + "ps");
        removeColumnButton.getStyle().set("background-color", "#ABB6B1");
        changeColumnLayout.add(addColumnButton, removeColumnButton);
        changeColumnLayout.setWidth("100px");
        updateEntityCounts();
        entityCountsLayout.setWidth("700px");
        entityCountsLayout.add(entityCountsLabel, this.entityCounts);
        addColumnLayout.add(queryScroller, changeColumnLayout, entityCountsLayout);
        addRowLayout.add(addColumnLayout, changeRowLayout);
        tableLayout.add(label, addRowLayout);

        return tableLayout;
    }

    private Component buildSearchComponent()
    {
        VerticalLayout leftColumnLayout = new VerticalLayout();
        ComboBox<String> entitySimilarities = new ComboBox<>("Entity similarity");
        IntegerField topKField = new IntegerField("Top-K");
        entitySimilarities.setItems("RDF types", "Predicates", "Embeddings");
        entitySimilarities.setRenderer(new ComponentRenderer<>(item -> {
            Span span = new Span(item);
            span.addClassNames("drop-down-items");
            return span;
        }));
        entitySimilarities.setClassName("combo-box");
        topKField.setRequiredIndicatorVisible(true);
        topKField.setMin(1);
        topKField.setMax(500000);
        topKField.setValue(10);
        topKField.setStepButtonsVisible(true);
        leftColumnLayout.add(entitySimilarities, topKField);

        VerticalLayout middleColumnLayout = new VerticalLayout();
        ComboBox<String> dataLakes = new ComboBox<>("Data lake");
        Checkbox prefilterBox = new Checkbox("Pre-filter", false);
        dataLakes.setItems(ConfigReader.dataLakes());
        dataLakes.setRenderer(new ComponentRenderer<>(item -> {
            Span span = new Span(item);
            span.addClassNames("drop-down-items");
            return span;
        }));
        dataLakes.setClassName("combo-box");
        middleColumnLayout.add(dataLakes, prefilterBox);

        VerticalLayout rightColumnLayout = new VerticalLayout();
        Button searchButton = new Button("Search", event -> search(topKField.getValue(), entitySimilarities.getValue(),
                dataLakes.getValue(), prefilterBox.getValue()));
        searchButton.setWidth("200px");
        searchButton.setHeight("80px");
        searchButton.getStyle().set("background-color", "#57AF34");
        searchButton.setClassName("search-button");
        rightColumnLayout.add(searchButton);

        return new HorizontalLayout(leftColumnLayout, middleColumnLayout, rightColumnLayout);
    }

    private void addRow()
    {
        int columns = !this.query.isEmpty() ? this.query.get(0).size() : 3;
        List<StringBuilder> newRow = new ArrayList<>(columns);

        for (int column = 0; column < columns; column++)
        {
            newRow.add(new StringBuilder());
        }

        this.query.add(newRow);
        buildQueryTable();
    }

    private void removeRow()
    {
        if (this.query.size() > 1)
        {
            this.query.remove(this.query.size() - 1);
            buildQueryTable();
        }
    }

    private void addColumn()
    {
        int rows = this.query.size();

        for (int row = 0; row < rows; row++)
        {
            this.query.get(row).add(new StringBuilder());
        }

        buildQueryTable();
    }

    private void removeColumn()
    {
        if (this.query.get(0).size() > 1)
        {
            int rows = this.query.size();

            for (int row = 0; row < rows; row++)
            {
                this.query.get(row).remove(this.query.get(row).size() - 1);
            }

            buildQueryTable();
        }
    }

    private void buildQueryTable()
    {
        VerticalLayout tableRowLayout = new VerticalLayout();
        int rows = this.query.size(), textFieldWidth = 300, textFieldHeight = 25;

        for (int row = 0; row < rows; row++)
        {
            HorizontalLayout tableColumnLayout = new HorizontalLayout();
            int columns = this.query.get(row).size();

            for (int column = 0; column < columns; column++)
            {
                int rowCoordinate = row, columnCoordinate = column;
                String cellContent = this.query.get(row).get(column).toString();
                TextField textField = new TextField("", cellContent, "");
                textField.setWidth(textFieldWidth + "px");
                textField.setHeight(textFieldHeight + "px");
                textField.getStyle().set("margin", "1px");
                textField.getStyle().set("background-color", "#DADCDF");
                textField.addValueChangeListener(event -> {
                    String content = event.getValue();
                    int oldContentLength = this.query.get(rowCoordinate).get(columnCoordinate).length();
                    this.query.get(rowCoordinate).get(columnCoordinate).replace(0, oldContentLength, content);
                    updateEntityCounts();
                });
                tableColumnLayout.add(textField);
            }

            tableRowLayout.add(tableColumnLayout);
        }

        this.querySection.removeAll();
        this.querySection.add(tableRowLayout);
    }

    private void updateEntityCounts()
    {
        this.entityCounts.setHeight("200px");
        this.entityCounts.removeAllColumns();
        this.entityCounts.addComponentColumn(item -> {
            VerticalLayout layout = new VerticalLayout();
            layout.add(new Html("<div><b>Entity</b>" + item.getFirst() + "</div>"));
            layout.add(new Html("<div><b>Count</b>" + item.getSecond() + "</div>"));

            return layout;
        }).setHeader("Entity counts").setVisible(false);
        this.entityCounts.addColumn(Pair::getFirst).setHeader("Entity");
        this.entityCounts.addColumn(Pair::getSecond).setHeader("Count");
        this.entityCounts.setItems(tableContents());
    }

    private Set<Pair<String, Integer>> tableContents()
    {
        Set<Pair<String, Integer>> contents = new TreeSet<>(Comparator.comparing(Pair::getFirst));

        for (List<StringBuilder> row : this.query)
        {
            for (StringBuilder cell : row)
            {
                String content = cell.toString();

                if (!content.isEmpty())
                {
                    contents.add(new Pair<>(content, count(content)));
                }
            }
        }

        return contents;
    }

    private int count(String entity)
    {
        return 0;
    }

    private void search(int topK, String entitySimilarity, String dataLake, boolean prefilter)
    {
        if (topK <= 0 || entitySimilarity == null || dataLake == null)
        {
            return;
        }

        String dataLakeIp = ConfigReader.getIp(dataLake),
                username = ConfigReader.getUsername(dataLake),
                password = ConfigReader.getPassword(dataLake);
        User user = new User(username, password, true);
        TableSearch.EntitySimilarity similarity;
        Query query = parseQuery();

        if (entitySimilarity.equals("Embeddings"))
        {
            similarity = TableSearch.EntitySimilarity.EMBEDDINGS_ABS;
        }

        else if (entitySimilarity.equals("RDF types"))
        {
            similarity = TableSearch.EntitySimilarity.JACCARD_TYPES;
        }

        else if (entitySimilarity.equals("Predicates"))
        {
            similarity = TableSearch.EntitySimilarity.JACCARD_PREDICATES;
        }

        else
        {
            // TODO: Throw error
            System.out.println("Not recognized: (" + entitySimilarity + ")");
            similarity = null;
        }

        try
        {
            DataLakeService dl = new DataLakeService(dataLakeIp, user);
            Response pingResponse = dl.ping();

            if (pingResponse.getResponseCode() != 200)
            {
                // TODO: Handle connection error
                System.out.println("Connection error");
            }

            Result result = dl.search(query, topK, similarity, prefilter);
            Iterator<dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair<File, Double>> resultsIter = result.getResults();

            while (resultsIter.hasNext())
            {
                this.results.add(resultsIter.next());
            }

            refreshResults();
        }

        catch (RuntimeException e)
        {
            // TODO: Handle search error
            System.out.println("Error: " + e.getMessage());
        }
    }

    private Query parseQuery()
    {
        List<List<String>> queryAsList = this.query.stream().map(row -> row.stream()
                .map(StringBuilder::toString)
                .collect(Collectors.toList())).toList();
        Table<String> queryAsTable = new DynamicTable<>(queryAsList);

        return new TableQuery(queryAsTable);
    }

    private void refreshResults()
    {
        clearResults();

        this.resultComponent = buildResults();
        this.layout.add(this.resultComponent);
    }

    private void clearResults()
    {
        if (this.resultComponent != null)
        {
            this.layout.remove(this.resultComponent);
            this.resultComponent = null;
        }
    }

    private Component buildResults()
    {
        VerticalLayout resultLayout = new VerticalLayout();
        HorizontalLayout resultHeader = new HorizontalLayout(), subHeader = new HorizontalLayout();
        Section resultSection = new Section();
        H1 resultLabel = new H1("Results");
        H2 topKLabel = new H2("Top-" + this.results.size());
        Dialog stats = statsDialog();
        Button clearButton = new Button("Clear", event -> clearResults()),
                statsButton = new Button("Statistics", event -> stats.open());
        clearButton.getStyle().set("margin-left", "100px");
        clearButton.getStyle().set("background-color", "#FD7B7C");
        clearButton.getStyle().set("--vaadin-button-text-color", "white");
        statsButton.getStyle().set("background-color", "#00669E");
        statsButton.getStyle().set("--vaadin-button-text-color", "white");
        // TODO: Add results here to resultLayout
        resultHeader.add(resultLabel, clearButton);
        subHeader.add(topKLabel, stats, statsButton);
        resultLayout.add(resultHeader, subHeader);
        clearResults();
        resultSection.add(resultLayout);
        resultSection.getStyle().set("background-color", "#2C85B6");

        Scroller scroller = new Scroller(new Div(resultSection));
        scroller.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.JustifyContent.CENTER, LumoUtility.Display.FLEX);

        return scroller;
    }

    private Dialog statsDialog()
    {
        Dialog dialog = new Dialog("Statistics");
        VerticalLayout layout = new VerticalLayout();
        H4 runtime = new H4("Runtime: 0s"), reduction = new H4("Reduction: 0%");
        layout.add(runtime, reduction);
        dialog.add(layout);

        Button closeButton = new Button(new Icon("lumo", "cross"), event -> dialog.close());
        dialog.getHeader().add(closeButton);

        return dialog;
    }
}
