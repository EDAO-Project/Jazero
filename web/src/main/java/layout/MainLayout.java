package layout;

import com.vaadin.flow.component.applayout.AppLayout;
import com.vaadin.flow.component.applayout.DrawerToggle;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.Header;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.Scroller;
import com.vaadin.flow.component.sidenav.SideNav;
import com.vaadin.flow.component.sidenav.SideNavItem;
import com.vaadin.flow.router.HasDynamicTitle;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.jazero.web.view.LobbyView;

public class MainLayout extends AppLayout
{
    private H2 viewTitle;

    public MainLayout()
    {
        setPrimarySection(Section.DRAWER);
        addNavBarContent();
        addDrawerContent();
    }

    private void addNavBarContent()
    {
        var toggle = new DrawerToggle();
        toggle.setAriaLabel("Menu toggle");
        toggle.setTooltipText("Menu toggle");

        this.viewTitle = new H2();
        this.viewTitle.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.Margin.NONE, LumoUtility.Flex.GROW);

        var header = new Header(toggle, this.viewTitle);
        header.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.Display.FLEX, LumoUtility.Padding.End.MEDIUM,
                LumoUtility.Width.FULL);

        addToNavbar(false, header);
    }

    private void addDrawerContent()
    {
        var appName = new Span("Vaadin Chat");
        appName.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.Display.FLEX, LumoUtility.FontSize.LARGE,
                LumoUtility.FontWeight.SEMIBOLD, LumoUtility.Height.XLARGE, LumoUtility.Padding.Horizontal.MEDIUM);
        addToDrawer(appName, new Scroller(createSizeNav()));
    }

    private SideNav createSizeNav()
    {
        SideNav nav = new SideNav();
        nav.addItem(new SideNavItem("Lobby", LobbyView.class, VaadinIcon.BUILDING.create()));
        return nav;
    }

    private String getCurrentPageTitle()
    {
        if (getContent() == null)
        {
            return "";
        }

        else if (getContent() instanceof HasDynamicTitle titleHolder)
        {
            return titleHolder.getPageTitle();
        }

        else
        {
            var title = getContent().getClass().getAnnotation(PageTitle.class);
            return title == null ? "" : title.value();
        }
    }

    @Override
    protected void afterNavigation()
    {
        super.afterNavigation();
        this.viewTitle.setText(getCurrentPageTitle());
    }
}
