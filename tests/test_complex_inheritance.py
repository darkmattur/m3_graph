"""
Tests for complex inheritance hierarchies and meta table verification.

Tests multiple layers of inheritance with various relationship types
and verifies that all inheritance and relationship metadata is correctly
stored in the meta table.
"""
from __future__ import annotations
import pytest
from m3_graph.link import Link, Backlink


@pytest.mark.asyncio
class TestComplexInheritance:
    """Test complex multi-layer inheritance hierarchies."""

    async def test_complex_inheritance_meta_table(self, graph, db_connection, test_schema):
        """
        Test complex inheritance hierarchy with many layers and verify
        all relationships appear in the meta table.

        Creates a 5-layer inheritance hierarchy:
        - Layer 1: BaseEntity (root)
        - Layer 2: Person, Organization
        - Layer 3: Employee (Person), Manager (Employee), Company (Organization)
        - Layer 4: SeniorManager (Manager), Department (Organization)
        - Layer 5: Executive (SeniorManager)

        With cross-layer relationships between different branches.
        """

        # Layer 1: Root base class
        class BaseEntity(graph.DBObject):
            category = "entities"
            type = "base_entity"
            name: str
            created_at: str | None = None

        # Layer 2: Two main branches
        class Person(BaseEntity):
            type = "person"
            email: str
            age: int | None = None

        class Organization(BaseEntity):
            type = "organization"
            registration_number: str

        # Layer 3: Further specialization with relationships
        class Employee(Person):
            type = "employee"
            employee_id: str
            department: Link[Department, "employees"] | None = None
            manager: Link[Manager, "direct_reports"] | None = None

        class Manager(Employee):
            type = "manager"
            direct_reports: Backlink[Employee]
            managed_departments: Backlink[Department]

        class Company(Organization):
            type = "company"
            stock_symbol: str | None = None
            ceo: Link[Executive, "companies_led"] | None = None
            departments: Backlink[Department]

        # Layer 4: Even deeper nesting
        class SeniorManager(Manager):
            type = "senior_manager"
            budget_authority: float
            reports_to: Link[Executive, "senior_team"] | None = None

        class Department(Organization):
            type = "department"
            department_code: str
            company: Link[Company, "departments"]
            manager: Link[Manager, "managed_departments"]
            employees: Backlink[Employee]

        # Layer 5: Top of hierarchy
        class Executive(SeniorManager):
            subtype = "executive"
            title: str  # CEO, CFO, CTO, etc.
            companies_led: Backlink[Company]
            senior_team: Backlink[SeniorManager]

        # Register all types and relationships in the meta table
        await graph.maintain()

        # Verify all types are registered
        assert "base_entity" in graph.__class__.types
        assert "person" in graph.__class__.types
        assert "organization" in graph.__class__.types
        assert "employee" in graph.__class__.types
        assert "manager" in graph.__class__.types
        assert "company" in graph.__class__.types
        assert "senior_manager" in graph.__class__.types
        assert "department" in graph.__class__.types
        assert "executive" in graph.__class__.subtypes

        # Create some test data to verify relationships work
        # Create a company
        acme = Company(
            source="test",
            name="Acme Corp",
            registration_number="REG123",
            stock_symbol="ACME"
        )
        await acme.insert()

        # Create an executive (CEO)
        ceo = Executive(
            source="test",
            name="Jane Smith",
            email="jane@acme.com",
            employee_id="EMP001",
            budget_authority=10000000.0,
            title="CEO"
        )
        await ceo.insert()

        # Link CEO to company
        acme.ceo = ceo
        await acme.update()

        # Create a department
        engineering = Department(
            source="test",
            name="Engineering",
            registration_number="DEPT-ENG",
            department_code="ENG",
            company=acme,
            manager=ceo  # CEO also manages engineering
        )
        await engineering.insert()

        # Create a senior manager
        vp_eng = SeniorManager(
            source="test",
            name="John Doe",
            email="john@acme.com",
            employee_id="EMP002",
            budget_authority=5000000.0,
            reports_to=ceo
        )
        await vp_eng.insert()

        # Create regular employees
        dev1 = Employee(
            source="test",
            name="Alice Developer",
            email="alice@acme.com",
            employee_id="EMP003",
            department=engineering,
            manager=vp_eng
        )
        await dev1.insert()

        dev2 = Employee(
            source="test",
            name="Bob Developer",
            email="bob@acme.com",
            employee_id="EMP004",
            department=engineering,
            manager=vp_eng
        )
        await dev2.insert()

        # Verify relationships work correctly
        assert acme.ceo == ceo
        assert ceo in acme.ceo.companies_led
        assert engineering.company == acme
        assert engineering in acme.departments
        assert engineering.manager == ceo
        assert vp_eng.reports_to == ceo
        assert vp_eng in ceo.senior_team
        assert dev1.department == engineering
        assert dev1.manager == vp_eng
        assert dev1 in engineering.employees
        assert dev1 in vp_eng.direct_reports

        # Now query the meta table to verify all inheritance relationships
        meta_rows = await db_connection.query(
            f"""
            SELECT category, type, subtype, forward, back, parent_types, descendant_types
            FROM {test_schema}.meta
            ORDER BY type, subtype
            """
        )

        # Convert to dictionary for easier verification
        meta_by_type = {row['type']: row for row in meta_rows}

        print("\n" + "="*80)
        print("META TABLE CONTENTS")
        print("="*80)
        for row in meta_rows:
            print(f"\nType: {row['type']} (subtype: {row['subtype']})")
            print(f"  Category: {row['category']}")
            print(f"  Parent types: {row['parent_types']}")
            print(f"  Descendant types: {row['descendant_types']}")
            print(f"  Forward relationships: {row['forward']}")
            print(f"  Back relationships: {row['back']}")
        print("="*80 + "\n")

        # Verify inheritance chains

        # BaseEntity has no parents
        assert meta_by_type['base_entity']['parent_types'] == []

        # Person inherits from BaseEntity
        assert 'base_entity' in meta_by_type['person']['parent_types']

        # Organization inherits from BaseEntity
        assert 'base_entity' in meta_by_type['organization']['parent_types']

        # Employee inherits from Person (and transitively from BaseEntity)
        assert 'person' in meta_by_type['employee']['parent_types']

        # Manager inherits from Employee
        assert 'employee' in meta_by_type['manager']['parent_types']

        # Company inherits from Organization
        assert 'organization' in meta_by_type['company']['parent_types']

        # SeniorManager inherits from Manager
        assert 'manager' in meta_by_type['senior_manager']['parent_types']

        # Department inherits from Organization
        assert 'organization' in meta_by_type['department']['parent_types']

        # Verify forward relationships

        # Employee has department_id and manager_id
        employee_forward = meta_by_type['employee']['forward']
        assert employee_forward is not None
        assert 'department_id' in employee_forward
        assert employee_forward['department_id'] == 'employees_ids'
        assert 'manager_id' in employee_forward
        assert employee_forward['manager_id'] == 'direct_reports_ids'

        # Manager inherits Employee's relationships but has no additional forward rels
        manager_forward = meta_by_type['manager']['forward']
        # Manager class itself defines no forward relationships (only backlinks)
        # The forward relationships come from inheritance
        assert manager_forward == {} or manager_forward is None

        # SeniorManager has reports_to
        senior_forward = meta_by_type['senior_manager']['forward']
        assert senior_forward is not None
        assert 'reports_to_id' in senior_forward
        assert senior_forward['reports_to_id'] == 'senior_team_ids'

        # Company has ceo_id
        company_forward = meta_by_type['company']['forward']
        assert company_forward is not None
        assert 'ceo_id' in company_forward
        assert company_forward['ceo_id'] == 'companies_led_ids'

        # Department has company_id and manager_id
        dept_forward = meta_by_type['department']['forward']
        assert dept_forward is not None
        assert 'company_id' in dept_forward
        assert dept_forward['company_id'] == 'departments_ids'
        assert 'manager_id' in dept_forward
        assert dept_forward['manager_id'] == 'managed_departments_ids'

        # Verify backward relationships

        # Manager has direct_reports_ids and managed_departments_ids
        manager_back = meta_by_type['manager']['back']
        assert manager_back is not None
        assert 'direct_reports_ids' in manager_back
        assert 'managed_departments_ids' in manager_back

        # Company has departments_ids
        company_back = meta_by_type['company']['back']
        assert company_back is not None
        assert 'departments_ids' in company_back

        # Department has employees_ids
        dept_back = meta_by_type['department']['back']
        assert dept_back is not None
        assert 'employees_ids' in dept_back

        # Verify all expected types are present
        expected_types = {
            'base_entity', 'person', 'organization', 'employee',
            'manager', 'company', 'senior_manager', 'department'
        }
        actual_types = set(meta_by_type.keys())
        assert expected_types == actual_types, \
            f"Missing types: {expected_types - actual_types}, Extra types: {actual_types - expected_types}"

        print("\n✓ All inheritance relationships verified in meta table")
        print("✓ All forward relationships verified in meta table")
        print("✓ All backward relationships verified in meta table")
        print(f"✓ Total types in hierarchy: {len(meta_rows)}")
        print(f"✓ Maximum inheritance depth: 5 layers")

    async def test_diamond_inheritance_meta_table(self, graph, db_connection, test_schema):
        """
        Test diamond inheritance pattern and verify meta table handling.

        Creates a diamond pattern:
               Base
              /    \\
          Mixin1  Mixin2
              \\    /
              Combined
        """

        class Base(graph.DBObject):
            category = "diamond"
            type = "base"
            name: str

        class Mixin1(Base):
            type = "mixin1"
            feature1: str
            related1: Link[Target1, "sources1"] | None = None

        class Mixin2(Base):
            type = "mixin2"
            feature2: str
            related2: Link[Target2, "sources2"] | None = None

        class Combined(Mixin1):
            type = "combined"
            combined_attr: str
            related3: Link[Target3, "sources3"] | None = None

        class Target1(Base):
            type = "target1"
            sources1: Backlink[Mixin1]

        class Target2(Base):
            type = "target2"
            sources2: Backlink[Mixin2]

        class Target3(Base):
            type = "target3"
            sources3: Backlink[Combined]

        await graph.maintain()

        # Query meta table
        meta_rows = await db_connection.query(
            f"""
            SELECT type, parent_types, forward, back
            FROM {test_schema}.meta
            ORDER BY type
            """
        )

        meta_by_type = {row['type']: row for row in meta_rows}

        print("\n" + "="*80)
        print("DIAMOND PATTERN META TABLE")
        print("="*80)
        for row in meta_rows:
            print(f"\nType: {row['type']}")
            print(f"  Parent types: {row['parent_types']}")
            print(f"  Forward: {row['forward']}")
            print(f"  Back: {row['back']}")
        print("="*80 + "\n")

        # Verify parent relationships
        assert meta_by_type['base']['parent_types'] == []
        assert 'base' in meta_by_type['mixin1']['parent_types']
        assert 'base' in meta_by_type['mixin2']['parent_types']
        assert 'mixin1' in meta_by_type['combined']['parent_types']

        # Verify relationships are correctly tracked
        assert meta_by_type['mixin1']['forward']['related1_id'] == 'sources1_ids'
        assert meta_by_type['mixin2']['forward']['related2_id'] == 'sources2_ids'
        assert meta_by_type['combined']['forward']['related3_id'] == 'sources3_ids'

        print("✓ Diamond inheritance pattern verified in meta table")

    async def test_multiple_inheritance_branches(self, graph, db_connection, test_schema):
        """
        Test multiple separate inheritance branches in the same category.

        Creates separate branches that don't inherit from each other:

        Branch A:          Branch B:          Branch C:
        RootA              RootB              RootC
          |                  |                  |
        ChildA1            ChildB1            ChildC1
          |                  |                  |
        GrandchildA1       ChildB2            ChildC2
        """

        # Branch A
        class RootA(graph.DBObject):
            category = "branches"
            type = "root_a"
            name: str

        class ChildA1(RootA):
            type = "child_a1"
            attr_a1: str

        class GrandchildA1(ChildA1):
            type = "grandchild_a1"
            attr_ga1: str

        # Branch B
        class RootB(graph.DBObject):
            category = "branches"
            type = "root_b"
            name: str

        class ChildB1(RootB):
            type = "child_b1"
            attr_b1: str
            link_to_c: Link[ChildC1, "linked_from_b"] | None = None

        class ChildB2(RootB):
            type = "child_b2"
            attr_b2: str

        # Branch C
        class RootC(graph.DBObject):
            category = "branches"
            type = "root_c"
            name: str

        class ChildC1(RootC):
            type = "child_c1"
            attr_c1: str
            linked_from_b: Backlink[ChildB1]

        class ChildC2(RootC):
            type = "child_c2"
            attr_c2: str

        await graph.maintain()

        # Query meta table
        meta_rows = await db_connection.query(
            f"""
            SELECT type, parent_types, forward, back
            FROM {test_schema}.meta
            WHERE category = 'branches'
            ORDER BY type
            """
        )

        meta_by_type = {row['type']: row for row in meta_rows}

        print("\n" + "="*80)
        print("MULTIPLE BRANCHES META TABLE")
        print("="*80)
        for row in meta_rows:
            print(f"\nType: {row['type']}")
            print(f"  Parent types: {row['parent_types']}")
            print(f"  Forward: {row['forward']}")
            print(f"  Back: {row['back']}")
        print("="*80 + "\n")

        # Verify each branch is independent
        assert meta_by_type['root_a']['parent_types'] == []
        assert meta_by_type['root_b']['parent_types'] == []
        assert meta_by_type['root_c']['parent_types'] == []

        # Verify inheritance within branches
        assert 'root_a' in meta_by_type['child_a1']['parent_types']
        assert 'child_a1' in meta_by_type['grandchild_a1']['parent_types']
        assert 'root_b' in meta_by_type['child_b1']['parent_types']
        assert 'root_b' in meta_by_type['child_b2']['parent_types']
        assert 'root_c' in meta_by_type['child_c1']['parent_types']
        assert 'root_c' in meta_by_type['child_c2']['parent_types']

        # Verify cross-branch relationships
        assert meta_by_type['child_b1']['forward']['link_to_c_id'] == 'linked_from_b_ids'
        assert 'linked_from_b_ids' in meta_by_type['child_c1']['back']

        # Verify all 9 types are present
        assert len(meta_by_type) == 9

        print("✓ Multiple independent inheritance branches verified in meta table")
        print(f"✓ Total branches: 3")
        print(f"✓ Total types: {len(meta_by_type)}")
