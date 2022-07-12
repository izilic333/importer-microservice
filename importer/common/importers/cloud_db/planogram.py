import psycopg2
from collections import OrderedDict
import pandas as pd
from common.logging.setup import logger
from common.importers.cloud_db.common import get_alive_and_dead, get_values_from_dict_arr, get_upsert_if_needed, \
    BaseImportHandler, BaseImportObject, BaseImporter, ReferenceField, IdField, Field
from common.importers.cloud_db.planogram_helpers import PlanogramHandler, PlanogramValidation, planogram_processor
from common.mixin.enum_errors import enum_message_on_specific_language
from common.mixin.validation_const import ImportType, ImportAction
from common.mixin.enum_errors import EnumValidationMessage as Const
from common.validators.cloud_db.common_validators import BaseValidator
from database.cloud_database.core.query import PlanogramQueryOnCloud, ProductQueryOnCloud, \
    ProductRotationGroupQueryOnCloud


class PlanogramObject(BaseImportObject):
    DB_TABLE = 'product_templates'

    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict([
            ('id', IdField(db_obj['id'] if db_obj else None)),
            ('caption', Field(mandatory=True, value=obj['caption'])),
            ('alive', Field(mandatory=False, default=True, value=alive)),
            ('owner_id', Field(mandatory=True, value=company_id)),
            ('author_info_id', Field(mandatory=False, default='/N')),
            ('enabled', Field(mandatory=False, default=True, value=True)),
            ('deleted', Field(mandatory=False, default=False, value=False)),
            ('external_id', Field(mandatory=True, value=obj['external_id'])),
            ('product_warning_percentage', Field(mandatory=False, value=obj['product_warning_percentage'])),
            ('component_warning_percentage', Field(mandatory=False, value=obj['component_warning_percentage'])),
            ('mail_notification', Field(mandatory=False, value=obj['mail_notification'])),
            ('pricelist_count', Field(mandatory=False, value=obj['pricelist_count'])),
        ])

        self.db_obj = db_obj


class LayoutColumnsObject(BaseImportObject):
    DB_TABLE = 'layout_columns'

    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict([
            ('id', IdField(db_obj['column_id'] if db_obj else None)),
            ('index', Field(mandatory=True, value=obj['index'])),
            ('alive', Field(mandatory=False, default=True, value=alive)),
            ('owner_id', Field(mandatory=True, value=company_id)),
            ('author_info_id', Field(mandatory=False, default='/N')),
            ('recipe_id', Field(mandatory=False, default='/N', value=obj['recipe_id'] if obj['recipe_id'] else '/N')),
            ('max_quantity', Field(mandatory=False, default=0, value=obj['max_quantity'])),
            ('minimum_route_pickup', Field(mandatory=False, default=0, value=obj['minimum_route_pickup'])),
            ('layout_id', ReferenceField(value=obj['planogram_id'],
                                         ref_type='layout',
                                         ref_field='id',
                                         ref_data=[('external_id', obj['external_id'])])),
            ('product_id', Field(mandatory=False, default='/N', value=obj['product_id'] if obj['product_id'] else '/N')),
            ('warning_quantity', Field(mandatory=False, value=obj['warning_quantity'])),
            ('next_fill_quantity', Field(mandatory=False, value=obj['next_fill_quantity'])),
            ('external_id', Field(mandatory=False, value=obj['external_id'])),
            ('price', Field(mandatory=True, value=obj['price'] if obj['price'] not in ["", None] else '/N')),
            ('price_2', Field(mandatory=False, default='/N', value=obj['price_2'] if obj['price_2'] not in ["", None] else '/N')),
            ('price_3', Field(mandatory=False, default='/N', value=obj['price_3'] if obj['price_3'] not in ["", None] else '/N')),
            ('price_4', Field(mandatory=False, default='/N', value=obj['price_4'] if obj['price_4'] not in ["", None]  else '/N')),
            ('price_5', Field(mandatory=False, default='/N', value=obj['price_5'] if obj['price_5'] not in ["", None] else '/N')),
            ('notify_warning', Field(mandatory=False, value=obj['notify_warning'])),
            ('combo_recipe_id', Field(mandatory=False, default='/N', value=obj['combo_recipe_id'] if obj['combo_recipe_id'] else '/N')),
            ('product_rotation_group_id', Field(mandatory=False, default='/N', value=obj['product_rotation_group_id'] if obj['product_rotation_group_id'] else '/N')),

            ('alarm_quantity', Field(mandatory=False, default='/N', value=0)),
        ])

        self.db_obj = db_obj


class LayoutComponentsObject(BaseImportObject):
    DB_TABLE = 'layout_components'

    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict([
            ('id', IdField(db_obj['id'] if db_obj else None)),
            ('alive', Field(mandatory=False, default=True, value=alive)),
            ('author_info_id', Field(mandatory=False, default='/N')),
            ('max_quantity', Field(mandatory=False, default=0, value=obj['component_max_quantity'])),
            ('warning_quantity', Field(mandatory=False, value=obj['component_warning_quantity'])),
            ('component_id', Field(mandatory=False, value=obj['product_component_id'])),
            ('notify_warning', Field(mandatory=False, value=obj['component_notify_warning'])),
            ('next_fill_quantity', Field(mandatory=False, value=obj['component_next_fill_quantity'])),
            ('tags', Field(mandatory=False, value=obj['component_tags'])),
            ('external_id', Field(mandatory=False, value=obj['external_id'])),
            ('layout_id', ReferenceField(value=obj['planogram_id'],
                                         ref_type='layout',
                                         ref_field='id',
                                         ref_data=[('external_id', obj['external_id'])])),
        ])

        self.db_obj = db_obj


class TagsObject(BaseImportObject):
    DB_TABLE = 'tags'

    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict([
            ('id', IdField(db_obj['id'] if db_obj else None)),
            ('alive', Field(mandatory=False, default=True, value=alive)),
            ('caption', Field(mandatory=False, value=obj['tags_caption'])),
            ('external_id', Field(mandatory=False, value=obj['external_id'])),
            ('index', Field(mandatory=True, value=obj['index'])),

        ])

        self.db_obj = db_obj


class LayoutColumnsTagsObject(BaseImportObject):
    DB_TABLE = 'layout_columns_tags'

    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict([
            ('id', IdField(db_obj['columns_tags_id'] if db_obj else None)),
            ('external_id', Field(mandatory=False, value=obj['external_id'])),
            ('index', Field(mandatory=False, value=obj['index'])),
            ('caption', Field(mandatory=False, value=obj['tags_caption'])),
            ('external_id', Field(mandatory=False, value=obj['external_id'])),
            ('layoutcolumn_id', ReferenceField(value=obj['column_id'], ref_type='layoutcolumn', ref_field='id',
                                               ref_data=[('index', obj['index']), ('external_id', obj['external_id'])])),
            ('tags_id', ReferenceField(value=obj['tags_id'], ref_type='tags', ref_field='id',
                                       ref_data=[('index', obj['index']), ('external_id', obj['external_id'])])),

        ])

        self.db_obj = db_obj


class PlanogramImportHandler(BaseImportHandler):
    DB_TABLE = 'product_templates'
    IMPORT_TYPE = PlanogramObject
    ACTION = 'import_action'
    DB_ID = 'ext_id'
    IMPORT_ID = 'external_id'
    CAPTION = 'caption'
    SEQ_NAME = 'product_templates_id_seq'

    def get_columns_to_update(self):
        return ['id', 'external_id', 'caption', 'alive', 'enabled', 'deleted', 'product_warning_percentage',
                'component_warning_percentage', 'mail_notification', 'pricelist_count']

    def get_columns_to_insert(self):
        return ['id', 'caption', 'alive', 'owner_id', 'external_id', 'enabled', 'deleted', 'product_warning_percentage',
                'component_warning_percentage', 'mail_notification', 'pricelist_count']

    def get_all_objs_from_database(self, company_id, external_ids=None):
        planogram_data = []
        if external_ids:
            planogram_data = external_ids
        return planogram_data


class LayoutColumnImportHandler(BaseImportHandler):
    DB_TABLE = 'layout_columns'
    IMPORT_TYPE = LayoutColumnsObject
    ACTION = 'column_action'
    DB_ID = 'column_id'
    IMPORT_ID = 'column_id'
    CAPTION = 'caption'
    SEQ_NAME = 'layout_columns_id_seq'

    def get_columns_to_update(self):
        return ['id', 'index', 'recipe_id', 'alive', 'max_quantity', 'product_id', 'warning_quantity',
                'alarm_quantity', 'next_fill_quantity', 'price', 'price_2', 'price_3', 'price_4',
                'price_5', 'notify_warning', 'combo_recipe_id', 'layout_id', 'product_rotation_group_id',
                'minimum_route_pickup']

    def get_columns_to_insert(self):
        return ['id', 'index', 'recipe_id', 'alive', 'max_quantity', 'product_id', 'warning_quantity',
                'alarm_quantity', 'next_fill_quantity', 'price', 'price_2', 'price_3', 'price_4',
                'price_5', 'notify_warning', 'combo_recipe_id', 'layout_id', 'product_rotation_group_id',
                'minimum_route_pickup']

    def get_all_objs_from_database(self, company_id, external_ids=None):
        column_data = []
        if external_ids:
            column_data = external_ids
        return column_data


class LayoutComponentImportHandler(BaseImportHandler):
    DB_TABLE = 'layout_components'
    IMPORT_TYPE = LayoutComponentsObject
    ACTION = 'component_action'
    DB_ID = 'id'
    IMPORT_ID = 'component_id'
    CAPTION = 'caption'
    SEQ_NAME = 'layout_components_id_seq'

    def get_columns_to_update(self):
        return ['id', 'alive', 'max_quantity', 'warning_quantity', 'next_fill_quantity', 'notify_warning', 'layout_id',
                'tags', 'component_id']

    def get_columns_to_insert(self):
        return ['id', 'alive', 'max_quantity', 'warning_quantity', 'next_fill_quantity', 'notify_warning', 'layout_id',
                'tags', 'component_id']

    def get_all_objs_from_database(self, company_id, external_ids=None):
        component_data = []
        if external_ids:
            component_data = external_ids
        return component_data


class TagsImportHandler(BaseImportHandler):
    DB_TABLE = 'tags'
    IMPORT_TYPE = TagsObject
    ACTION = 'tags_action'
    DB_ID = 'id'
    IMPORT_ID = 'tags_id'
    CAPTION = 'caption'
    SEQ_NAME = 'tags_id_seq'

    def get_columns_to_update(self):
        return ['id', 'alive', 'caption']

    def get_columns_to_insert(self):
        return ['id', 'alive', 'caption']

    def get_all_objs_from_database(self, company_id, external_ids=None):
        tags_data = []
        if external_ids:
            tags_data = external_ids
        return tags_data


class LayoutColumnsTagsImportHandler(BaseImportHandler):
    DB_TABLE = 'layout_columns_tags'
    IMPORT_TYPE = LayoutColumnsTagsObject
    ACTION = 'tags_action'
    DB_ID = 'columns_tags_id'
    IMPORT_ID = 'columns_tags_id'
    CAPTION = 'caption'
    SEQ_NAME = 'layout_columns_tags_id_seq'

    def get_columns_to_update(self):
        return ['id', 'layoutcolumn_id', 'tags_id']

    def get_columns_to_insert(self):
        return ['id', 'layoutcolumn_id', 'tags_id']

    def get_all_objs_from_database(self, company_id, external_ids=None):
        planogram_data = []
        if external_ids:
            planogram_data = external_ids
        return planogram_data


class PlanogramImporter(BaseImporter):
    def populate(self, data, external_ids=None):
        planogram_for_import = data['planogram_for_import']
        column_for_import = data['column_for_import']
        component_for_import = data['component_for_import']
        tags_for_import = data['tags_for_import']
        database_product_templates = data['database_cloud_product_templates']
        database_columns = data['database_columns']
        database_layout_columns_tags = data['database_layout_columns_tags']
        database_layout_component = data['database_layout_component']
        import_objects = []

        # populate planogram
        if planogram_for_import:
            import_objects = PlanogramImportHandler(planogram_for_import, self.company_id, database_product_templates)
            self.all_import_handlers.append(import_objects)
            import_objects.populate_insert_objs_ids()

        if column_for_import:
            # populate column
            column_import_objects = LayoutColumnImportHandler(column_for_import, self.company_id, database_columns)
            self.all_import_handlers.append(column_import_objects)
            column_import_objects.populate_insert_objs_ids()
            column_import_objects.populate_ref_fields('layout', import_objects.objs_to_insert)

            # populate tags
            if tags_for_import:
                tags_import_objects = TagsImportHandler(tags_for_import, self.company_id, database_layout_columns_tags)
                self.all_import_handlers.append(tags_import_objects)
                tags_import_objects.populate_insert_objs_ids()

                # populate layout_columns_tags
                layout_columns_tags_import_objects = LayoutColumnsTagsImportHandler(
                    column_for_import, self.company_id, database_layout_columns_tags)
                self.all_import_handlers.append(layout_columns_tags_import_objects)
                layout_columns_tags_import_objects.populate_insert_objs_ids()
                layout_columns_tags_import_objects.populate_ref_fields('tags', tags_import_objects.objs_to_insert)
                layout_columns_tags_import_objects.populate_ref_fields('layoutcolumn', column_import_objects.objs_to_insert)

        # populate component
        if component_for_import:
            component_import_objects = LayoutComponentImportHandler(
                component_for_import, self.company_id, database_layout_component)
            self.all_import_handlers.append(component_import_objects)
            component_import_objects.populate_insert_objs_ids()
            component_import_objects.populate_ref_fields('layout', import_objects.objs_to_insert)

    def get_stats(self):
        return self.stats['product_templates']


class PlanogramValidator(BaseValidator):
    __name__ = 'planogram'

    def __init__(self, company_id, working_data, language):
        self.company_id = company_id
        self.working_data = working_data
        self.language = language
        self.warnings = []
        self.errors = []
        self.removed_import_row = []

    def append_error(self, record_part, message_part):
        self.errors.append({
            'record': record_part,
            'message': message_part
        })

    def append_warning(self, record_part, message_part):
        self.warnings.append({
            'record': record_part,
            'message': message_part
        })

    def message_translator(self, const, *args):
        return enum_message_on_specific_language(const.value, self.language, *args)

    def planogram_init_data_from_database(self, work_on):
        # get init data from database
        logger.info(self.message_translator(Const.GET_DATA_FOR_PLANOGRAM, 'product_templates', 'products',
                                            'combo_recipe_data', 'layout_columns_tags', 'recipe_data',
                                            'rotation_groups', 'company_prices'))

        planograms = PlanogramQueryOnCloud.get_planogram_for_company(self.company_id)
        products = ProductQueryOnCloud.get_products_for_company(self.company_id)
        combo_recipes = PlanogramQueryOnCloud.get_combo_recipe(self.company_id)
        layout_columns_tags = PlanogramQueryOnCloud.get_layout_column_tags()
        recipes = PlanogramQueryOnCloud.get_recipe(self.company_id)
        rotation_groups = ProductRotationGroupQueryOnCloud.get_product_rotation_groups_for_company(self.company_id)
        company_prices = PlanogramQueryOnCloud.company_price_definition(self.company_id)
        if not company_prices:
            company_prices = ['price_1']

        logger.info(self.message_translator(Const.SUCCESS_FETCHED_PLANOGRAM_DATA))
        logger.info(self.message_translator(Const.PLANOGRAM_START_VALIDATION))

        if not planograms['status']:
            self.append_warning(work_on, self.message_translator(Const.DATABASE_QUERY_ERROR, 'product_templates'))

        elif not products['status'] or not products['results']:
            self.append_error(work_on, self.message_translator(Const.DATABASE_QUERY_ERROR, 'products'))
            self.append_error(work_on, self.message_translator(Const.NO_PRODUCTS_ON_COMPANY, self.company_id))

        elif not rotation_groups.get('status'):
            self.append_warning(work_on, self.message_translator(Const.DATABASE_QUERY_ERROR, 'rotation_groups'))

        elif not recipes['status']:
            self.append_warning(work_on, self.message_translator(Const.DATABASE_QUERY_ERROR, 'recipe_data'))

        elif not combo_recipes['status']:
            self.append_warning(work_on, self.message_translator(Const.DATABASE_QUERY_ERROR, 'combo_recipe_data'))

        data = {
            'recipes': recipes['results'],
            'products': products['results'],
            'planograms': planograms['results'],
            'combo_recipes': combo_recipes['results'],
            'rotation_groups': rotation_groups['results'],
            'layout_columns_tags': layout_columns_tags,
            'company_prices': company_prices,

        }
        return data

    def validate(self):
        import_stats = {

            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "errors": 0
        }
        work_on = ImportType.PLANOGRAMS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        action_fld = work_on.lower() + '_action'
        empty_header = '<null>'
        removed_import_rows = []
        database_validation_errors_count = 0

        # get init data from database
        logger.info(self.message_translator(Const.FETCH_PLANOGRAM_DATA))
        data_from_db = self.planogram_init_data_from_database(work_on)

        cloud_planograms_all = data_from_db['planograms']
        all_company_product = data_from_db['products']
        combo_recipe_data = data_from_db['combo_recipes']
        layout_columns_tags = data_from_db['layout_columns_tags']
        recipe_data = data_from_db['recipes']
        pr_rotation_groups = data_from_db['rotation_groups']
        company_prices = data_from_db['company_prices']

        logger.info(self.message_translator(Const.SUCCESS_FETCHED_PLANOGRAM_DATA))
        if self.errors:
            return self.errors, self.warnings, import_stats, database_validation_errors_count

        # planogram data
        logger.info(self.message_translator(Const.PLANOGRAM_MAIN_FILTER))
        planogram_name_and_ext_id = []
        all_alive_cloud_planograms = []
        all_alive_cloud_planogram_ids = []
        if cloud_planograms_all:
            pl_data_frame = pd.DataFrame(cloud_planograms_all)
            pl_data_frame = pl_data_frame.loc[pl_data_frame['alive']]
            planogram_name_and_ext_id = pl_data_frame[['name', 'ext_id']].iloc[:, 0].tolist()
            all_alive_cloud_planograms = [p for p in cloud_planograms_all if p['alive'] is True]
            all_alive_cloud_planogram_ids = get_values_from_dict_arr(all_alive_cloud_planograms, 'ext_id')

        external_ids = set([item[id_fld] for item in self.working_data])
        alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_planograms_all)

        # product rotation groups data
        pr_rotation_all_alive = []
        pr_rotation_group_data_frame_records = []
        if pr_rotation_groups:
            pr_rotation_group_data_frame = pd.DataFrame(pr_rotation_groups)
            pr_rotation_group_data_frame = pr_rotation_group_data_frame.loc[pr_rotation_group_data_frame['alive']]
            pr_rotation_all_alive = pr_rotation_group_data_frame['ext_id'].tolist()
            pr_rotation_group_data_frame_records = pr_rotation_group_data_frame.to_dict('records')

        # product data
        all_cloud_company_product_ext_id = []
        products_specific_field_data_frame = []
        alive_products = []
        if all_company_product:
            products_data_frame = pd.DataFrame(all_company_product)
            alive_company_products_data_frame = products_data_frame.loc[products_data_frame['alive']]
            alive_products = alive_company_products_data_frame.to_dict('records')
            all_cloud_company_product_ext_id = alive_company_products_data_frame['ext_id'].tolist()
            products_specific_field_data_frame = products_data_frame[['ext_id', 'is_composite', 'is_combo']]

        # recipe data
        recipe_data_code = []
        combo_recipe_data_code = []
        if recipe_data:
            recipe_data_code = [{'recipe_code': x['code'], 'product_ext_id': x['product_ext_id'], 'alive': x['alive']} for x in recipe_data]
            combo_recipe_data_code = [{'combo_recipe_code': x['code'], 'product_ext_id': x['product_ext_id'], 'combo_recipe_id': x['id'], 'alive': x['alive']} for x in combo_recipe_data]

        logger.info(self.message_translator(Const.PLANOGRAM_MAIN_FILTER_SUCCESS))
        planogram_handler = PlanogramHandler(
            all_company_planograms=all_alive_cloud_planograms, all_company_product=alive_products,
            planogram_name_and_external_id=planogram_name_and_ext_id, language=self.language,
            all_cloud_company_product_ext_id=all_cloud_company_product_ext_id,
            products_data_frame_object=products_specific_field_data_frame)

        # init data validation
        logger.info(self.message_translator(Const.INIT_PLANOGRAM_VALIDATION_START))
        planogram_column_repeat = planogram_handler.check_column_per_planogram(self.working_data)
        planogram_name_repeat = planogram_handler.check_planogram_name_per_external_id(self.working_data)
        planogram_ext_id_repeat = planogram_handler.check_planogram_external_id_per_planogram_name(self.working_data)

        init_validation_results, init_validation_warnings = planogram_handler.planogram_name_and_external_id_processor(
            planogram_column_repeat, planogram_name_repeat, planogram_ext_id_repeat)

        planogram_name_column_repeat = init_validation_results['remove_planogram_name_column_repeat']
        planogram_name_ext_id_repeat = init_validation_results['remove_planogram_name_ext_id_repeat']
        planogram_ext_id_repeat = init_validation_results['remove_planogram_ext_id_repeat']
        self.warnings = self.warnings + init_validation_warnings
        logger.info(self.message_translator(Const.INIT_PLANOGRAM_VALIDATION_SUCCESS))

        # initialize planogram validator
        planogram_validator = PlanogramValidation(
            work_on=work_on,
            language=self.language,
            empty_header=empty_header,
            alive_products=alive_products,
            company_id=self.company_id,
            recipe_data=recipe_data,
            pr_rotation_group_data_frame=pr_rotation_group_data_frame_records,
        )

        # planogram validation
        logger.info(self.message_translator(Const.CORE_PLANOGRAM_VALIDATION_START))
        for import_item in self.working_data:

            # if len(removed_import_rows) > 1000:
            #     self.append_error('planogram', self.message_translator(
            #         Const.PLANOGRAM_IMPORT_FILE_TO_MANY_INCORRECT_ROWS, 1000))
            #     return self.errors, self.warnings, import_stats

            planogram_ext_id = str(import_item['planogram_id'])
            planogram_name = str(import_item['planogram_name'])
            product_ext_id = str(import_item['product_id'])
            column = int(import_item['column_number'])
            multiple_price_lists = import_item.get('multiple_pricelists')
            multi_price = 1

            if multiple_price_lists and multiple_price_lists != empty_header:
                multi_price = int(float(multiple_price_lists)) if multiple_price_lists else 1

            import_item['multiple_pricelists'] = multi_price

            # planogram init validation for some fields
            init_validation_removed_row, warning_message = planogram_validator.planogram_init_validation(
                planogram_name_column_repeat, planogram_name_ext_id_repeat, planogram_ext_id_repeat, import_item)
            self.warnings = self.warnings + warning_message
            if init_validation_removed_row:
                removed_import_rows = removed_import_rows + init_validation_removed_row
                continue

            try:
                action = ImportAction(int(import_item[action_fld]))
                action, import_item[action_fld] = get_upsert_if_needed(action, planogram_ext_id, alive_ids)
            except ValueError:
                self.append_error(import_item[id_fld],
                                  self.message_translator(Const.DATABASE_WRONG_ACTION, work_on, import_item[action_fld]))
                continue
            # planogram multi price validation
            price_errors, price_warnings, removed_import_row_price_fail = planogram_validator.validate_planogram_multi_price(
                company_prices, multi_price, import_item)

            if price_errors:
                self.errors = self.errors + price_errors
                return self.errors, self.warnings, import_stats, database_validation_errors_count

            self.warnings = self.warnings + price_warnings
            if removed_import_row_price_fail:
                removed_import_rows = removed_import_rows + removed_import_row_price_fail
                continue

            # Check if planogram already exist on cloud
            if action in [ImportAction.CREATE, ImportAction.UPDATE]:
                check_status = planogram_handler.check_exists_planogram_in_company(
                    planogram_name=planogram_name, planogram_external_id=planogram_ext_id)
                if not check_status["search_results"]:

                    if check_status["planogram_name_exists"]:
                        message_planogram_name_exist = {
                            'record': planogram_ext_id,
                            'message': self.message_translator(Const.PLANOGRAM_NAME_ALREADY_EXISTS_ON_CLOUD, planogram_name)
                        }
                        if message_planogram_name_exist not in self.warnings:
                            self.warnings.append(message_planogram_name_exist)
                            database_validation_errors_count += 1
                        removed_import_rows.append(import_item)

                    elif check_status["planogram_external_id_exists"] and action == ImportAction.CREATE:
                        message_planogram_ext_id_exist = {
                            'record': planogram_ext_id,
                            'message': self.message_translator(Const.PLANOGRAM_EXTERNAL_ID_ALREADY_EXISTS_ON_CLOUD, planogram_ext_id)
                        }
                        if message_planogram_ext_id_exist not in self.warnings:
                            self.warnings.append(message_planogram_ext_id_exist)
                            database_validation_errors_count += 1
                        removed_import_rows.append(import_item)

            # validate product and product rotation
            product_existing_on_company_status = planogram_handler.check_exists_product_in_company(product_ext_id)
            composite_product, combo_product = planogram_handler.composite_and_combo_product_check(product_ext_id)
            product_errors, product_warnings, removed_import_row_product_fail, prg_id, import_item = planogram_validator.validate_planogram_product(
                product_existing_on_company_status, composite_product, combo_product, recipe_data_code,
                combo_recipe_data_code, pr_rotation_all_alive, import_item)
            # update import item with prg_id
            import_item['prg_id'] = prg_id

            self.warnings = self.warnings + product_warnings

            if product_errors:
                self.errors = self.errors + product_errors
                return self.errors, self.warnings, import_stats, database_validation_errors_count

            if removed_import_row_product_fail:
                removed_import_rows = removed_import_rows + removed_import_row_product_fail
                continue

            column_check = False
            result = []
            if action == ImportAction.CREATE and planogram_ext_id in all_alive_cloud_planogram_ids:
                result = list(filter(lambda planogram: str(planogram['ext_id']) == planogram_ext_id and str(
                    planogram['name']) == planogram_name, all_alive_cloud_planograms))
                if result:
                    planogram_id = result[0]['id']
                    planogram_columns = PlanogramQueryOnCloud.get_columns_for_planogram(planogram_id)
                    column_check = PlanogramHandler.check_product_column_on_planogram(column, planogram_columns)

            import_action_errors = planogram_validator.validate_planogram_import_action(
                action, alive_ids, all_alive_cloud_planogram_ids, column_check, result, import_item['planogram_id'])

            if import_action_errors:
                self.errors = self.errors + import_action_errors
                return self.errors, self.warnings, import_stats, database_validation_errors_count

        for item in removed_import_rows:
            if item in self.working_data:
                self.working_data.remove(item)

        if len(self.working_data) == 0:
            self.append_error(work_on, self.message_translator(Const.ALL_ENTITIES_REMOVED))
            return self.errors, self.warnings, import_stats, database_validation_errors_count

        logger.info(self.message_translator(Const.PLANOGRAM_FINISH_VALIDATION))
        logger.info(self.message_translator(
            Const.PLANOGRAM_FETCH_INIT_DATA, 'planograms_columns', 'layout_component', 'product_components')
        )

        planograms_columns = PlanogramQueryOnCloud.get_planogram_columns(all_alive_cloud_planogram_ids, self.company_id)
        layout_component = PlanogramQueryOnCloud.get_layout_component()
        product_components = PlanogramQueryOnCloud.get_product_component(self.company_id)
        logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_BUILD_IMPORT_ENTITY))

        planograms, columns, components, tags = planogram_processor(
            data=self.working_data,
            planograms_columns=planograms_columns,
            product_components=product_components,
            layout_components=layout_component,
            layout_columns_tags=layout_columns_tags
        )

        import_data = {
            'planogram_for_import': planograms,
            'column_for_import': columns,
            'component_for_import': components,
            'tags_for_import': tags,
            'database_cloud_product_templates': cloud_planograms_all,
            'database_columns': planograms_columns,
            'database_layout_columns_tags': layout_columns_tags,
            'database_layout_component': layout_component,
        }
        logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_SUCCESS_BUILD_IMPORT_ENTITY))
        logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_POPULATE_IMPORT_OBJECTS))

        importer = PlanogramImporter(self.company_id)
        importer.populate(import_data, all_alive_cloud_planogram_ids)

        logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_FINISH_POPULATE_IMPORT_OBJECTS))

        try:
            logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_CALL_PL_SQL_PROCEDURE))
            import_stats = importer.save()
        except psycopg2.DatabaseError as e:

            logger.error(self.message_translator(Const.ERROR_ON_PLANOGRAM_SAVE, e))

            self.append_error('planogram', self.message_translator(Const.IMPORT_FAIL, 'planograms'))
            return self.errors, self.warnings, import_stats, database_validation_errors_count

        logger.info(self.message_translator(Const.PLANOGRAM_IMPORTER_PL_SQL_PROCEDURE_FINISHED))
        return self.errors, self.warnings, import_stats, database_validation_errors_count
